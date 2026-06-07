//! `lmxd` — a live-mutex-mills node daemon over TCP.
//!
//! Usage:
//!   lmxd [--codec text|json|msgpack] <my_id> <addr_0> <addr_1> ... <addr_{n-1}>
//!
//! `my_id` indexes into the address list. Start one process per node, e.g.:
//!   lmxd 0 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!   lmxd 1 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!   lmxd 2 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!
//! Then type commands on stdin:
//!   acquire <lock>   release <lock>   quit

use std::io::BufRead;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use live_mutex_mills::codec::WireCodec;
use live_mutex_mills::composite::{Composite, Progress};
use live_mutex_mills::transport::{run_node_with_codec, Command, NodeEvent};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "usage: lmxd [--codec text|json|msgpack] <my_id> <addr_0> <addr_1> ... <addr_{{n-1}}>"
        );
        std::process::exit(2);
    }
    let mut codec = WireCodec::Text;
    let mut offset = 1usize;
    if args.get(1).is_some_and(|s| s == "--codec") {
        let Some(raw) = args.get(2) else {
            eprintln!("missing codec after --codec; use text, json, or msgpack");
            std::process::exit(2);
        };
        codec = WireCodec::parse(raw).unwrap_or_else(|| {
            eprintln!("unknown codec {raw:?}; use text, json, or msgpack");
            std::process::exit(2);
        });
        offset = 3;
    } else if let Some(raw) = args.get(1).and_then(|s| s.strip_prefix("--codec=")) {
        codec = WireCodec::parse(raw).unwrap_or_else(|| {
            eprintln!("unknown codec {raw:?}; use text, json, or msgpack");
            std::process::exit(2);
        });
        offset = 2;
    }
    if args.len() < offset + 2 {
        eprintln!(
            "usage: lmxd [--codec text|json|msgpack] <my_id> <addr_0> <addr_1> ... <addr_{{n-1}}>"
        );
        std::process::exit(2);
    }
    let id: u32 = args[offset].parse().unwrap_or_else(|_| {
        eprintln!("my_id must be a non-negative integer");
        std::process::exit(2);
    });
    let addrs: Vec<String> = args[offset + 1..].to_vec();
    if id as usize >= addrs.len() {
        eprintln!("my_id {id} is outside the address list");
        std::process::exit(2);
    }

    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (evt_tx, evt_rx) = mpsc::channel();

    // Optional self-test workload (LMX_DEMO=1): every node repeatedly acquires
    // the SAME composite (multi-key) lock under contention, so the logs show
    // exclusive handoff across peers with strictly-increasing per-key fences.
    let demo_enabled = std::env::var("LMX_DEMO")
        .map(|v| !v.is_empty() && v != "0")
        .unwrap_or(false);
    let demo_keys: Vec<String> = std::env::var("LMX_DEMO_KEYS")
        .unwrap_or_else(|_| "cap,mid,zed".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let (demo_tx, demo_rx) = mpsc::channel::<(String, live_mutex_mills::Fence)>();

    // Print events (and feed acquisitions to the demo driver).
    thread::spawn(move || {
        for ev in evt_rx {
            match ev {
                NodeEvent::Acquired(lock, fence) => {
                    println!("ACQUIRED {lock} fence={fence}");
                    let _ = demo_tx.send((lock, fence));
                }
                NodeEvent::Lost(lock) => println!("LOST {lock}"),
                NodeEvent::Info(s) => println!("# {s}"),
            }
        }
    });

    // Demo driver: ordered composite acquire -> hold -> release, forever.
    if demo_enabled {
        let cmd = cmd_tx.clone();
        let keys = demo_keys.clone();
        thread::spawn(move || loop {
            let mut c = match Composite::new(&keys) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("invalid LMX_DEMO_KEYS: {e}");
                    return;
                }
            };
            if let Some(first) = c.pending() {
                let _ = cmd.send(Command::Acquire(first.clone()));
            }
            while !c.is_held() {
                let (lock, fence) = match demo_rx.recv() {
                    Ok(v) => v,
                    Err(_) => return,
                };
                match c.on_acquired(&lock, fence) {
                    Progress::Next(k) => {
                        let _ = cmd.send(Command::Acquire(k));
                    }
                    Progress::Held => {
                        let parts: Vec<String> = c
                            .keys()
                            .iter()
                            .map(|k| format!("{k}={}", c.fence(k).unwrap_or(0)))
                            .collect();
                        println!("DEMO COMPOSITE HELD [{}]", parts.join(", "));
                    }
                    Progress::Ignored => {}
                }
            }
            thread::sleep(Duration::from_millis(800));
            for k in c.keys() {
                let _ = cmd.send(Command::Release(k.clone()));
            }
            thread::sleep(Duration::from_millis(400));
        });
    } else {
        drop(demo_rx);
    }

    // Read stdin commands.
    thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            let mut it = line.split_whitespace();
            match it.next() {
                Some("acquire") => {
                    if let Some(l) = it.next() {
                        let _ = cmd_tx.send(Command::Acquire(l.to_string()));
                    }
                }
                Some("release") => {
                    if let Some(l) = it.next() {
                        let _ = cmd_tx.send(Command::Release(l.to_string()));
                    }
                }
                Some("quit") | Some("exit") => {
                    let _ = cmd_tx.send(Command::Shutdown);
                    break;
                }
                Some(other) => eprintln!(
                    "unknown command {other:?}; use: acquire <lock> | release <lock> | quit"
                ),
                None => {}
            }
        }
    });

    println!(
        "# node {id} of {} starting with {} codec",
        addrs.len(),
        codec.as_str()
    );
    if let Err(e) = run_node_with_codec(id, addrs, codec, cmd_rx, evt_tx) {
        eprintln!("node error: {e}");
        std::process::exit(1);
    }
}

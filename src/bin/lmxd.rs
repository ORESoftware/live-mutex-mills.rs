//! `lmxd` — a live-mutex-mills node daemon over TCP.
//!
//! Usage:
//!   lmxd <my_id> <addr_0> <addr_1> ... <addr_{n-1}>
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

use live_mutex_mills::transport::{run_node, Command, NodeEvent};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: lmxd <my_id> <addr_0> <addr_1> ... <addr_{{n-1}}>");
        std::process::exit(2);
    }
    let id: u32 = args[1].parse().expect("my_id must be a number");
    let addrs: Vec<String> = args[2..].to_vec();
    assert!((id as usize) < addrs.len(), "my_id out of range");

    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (evt_tx, evt_rx) = mpsc::channel();

    // Print events.
    thread::spawn(move || {
        for ev in evt_rx {
            match ev {
                NodeEvent::Acquired(lock, fence) => println!("ACQUIRED {lock} fence={fence}"),
                NodeEvent::Lost(lock) => println!("LOST {lock}"),
                NodeEvent::Info(s) => println!("# {s}"),
            }
        }
    });

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
                Some(other) => eprintln!("unknown command {other:?}; use: acquire <lock> | release <lock> | quit"),
                None => {}
            }
        }
    });

    println!("# node {id} of {} starting", addrs.len());
    if let Err(e) = run_node(id, addrs, cmd_rx, evt_tx) {
        eprintln!("node error: {e}");
        std::process::exit(1);
    }
}

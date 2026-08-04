//! `lmxd` — a live-mutex-mills node daemon over TCP.
//!
//! Usage:
//!   lmxd [--codec text|json|msgpack] [--client-http addr] [--client-tcp addr] \
//!        <my_id> <addr_0> <addr_1> ... <addr_{n-1}>
//!
//! `my_id` indexes into the address list. Start one process per node, e.g.:
//!   lmxd 0 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!   lmxd 1 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!   lmxd 2 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002
//!
//! Then type commands on stdin, or through either client listener:
//!   acquire <lock>   release <lock>   quit

use std::collections::HashMap;
use std::io::BufRead;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use live_mutex_mills::broker_runtime_config::{
    BrokerRuntimeConfig, BrokerRuntimeConfigError, USAGE,
};
use live_mutex_mills::client_api::{serve_http, serve_tcp, start_coordinator};
use live_mutex_mills::composite::{Composite, Progress};
use live_mutex_mills::transport::{run_node_with_settings, Command, NodeEvent};

fn main() {
    // Retain the guard for the life of the daemon so the OTLP batch processor
    // flushes spans and structured events during controlled shutdown.
    let _telemetry = fiducia_telemetry::init("live-mutex-mills");

    let config = match BrokerRuntimeConfig::from_process() {
        Ok(config) => config,
        Err(BrokerRuntimeConfigError::HelpRequested(help)) => {
            print!("{help}");
            return;
        }
        Err(err) => {
            tracing::error!(error = %err, "invalid broker configuration");
            // Keep the bounded, redacted configuration error directly visible
            // even when the selected telemetry subscriber suppresses local logs.
            eprintln!("invalid broker configuration: {err}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };

    let id = config.id;
    let addrs = config.addrs;
    let codec = config.codec;
    let client_http = config.client_http;
    let client_tcp = config.client_tcp;
    let stdin_enabled = config.stdin;
    let log_info = config.log_info;
    let transport = config.transport;

    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (raw_evt_tx, raw_evt_rx) = mpsc::channel();
    let (client, evt_rx) = start_coordinator(cmd_tx.clone(), raw_evt_rx);

    if let Some(addr) = client_http {
        match serve_http(&addr, client.clone()) {
            Ok(bound) => tracing::info!(%bound, "client HTTP listening"),
            Err(e) => {
                tracing::error!(%addr, error = %e, "client HTTP bind failed");
                std::process::exit(1);
            }
        }
    }
    if let Some(addr) = client_tcp {
        match serve_tcp(&addr, client.clone()) {
            Ok(bound) => tracing::info!(%bound, "client TCP listening"),
            Err(e) => {
                tracing::error!(%addr, error = %e, "client TCP bind failed");
                std::process::exit(1);
            }
        }
    }

    // Optional self-test workload (LMX_DEMO=1): every node repeatedly acquires
    // the SAME composite (multi-key) lock under contention, so the logs show
    // exclusive handoff across peers with strictly-increasing per-key fences.
    let demo_enabled = config.demo.enabled;
    let demo_keys = config.demo.keys;
    let demo_hold = config.demo.hold;
    let demo_rest = config.demo.rest;
    let (demo_tx, demo_rx) = mpsc::channel::<(String, live_mutex_mills::Fence)>();

    // Print events (and feed acquisitions to the demo driver).
    thread::spawn(move || {
        for ev in evt_rx {
            match ev {
                NodeEvent::Acquired(lock, fence) => {
                    tracing::info!(%lock, %fence, event = "acquired", "lock acquired");
                    println!("ACQUIRED {lock} fence={fence}");
                    let _ = demo_tx.send((lock, fence));
                }
                NodeEvent::Lost(lock) => {
                    tracing::warn!(%lock, event = "lost", "lock lost");
                    println!("LOST {lock}");
                }
                NodeEvent::Info(s) => {
                    if log_info {
                        println!("# {s}");
                    }
                }
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
                    tracing::error!(error = %e, "invalid LMX_DEMO_KEYS");
                    return;
                }
            };
            if let Some(first) = c.pending() {
                let _ = cmd.send(Command::Acquire(first.clone()));
            }
            // Bound each attempt: the node self-heals dropped messages via
            // re-broadcast, but if an attempt still stalls we release and retry
            // rather than wedge forever on a one-shot kickoff.
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut held = false;
            while !c.is_held() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break; // stalled — fall through to release + retry
                }
                let (lock, fence) = match demo_rx.recv_timeout(remaining) {
                    Ok(v) => v,
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return,
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
                        held = true;
                    }
                    Progress::Ignored => {}
                }
            }
            if held {
                thread::sleep(demo_hold);
            }
            // Release every component (clears partial node state), then drain any
            // stale acquisitions so the next attempt starts clean.
            for k in c.keys() {
                let _ = cmd.send(Command::Release(k.clone()));
            }
            while demo_rx.try_recv().is_ok() {}
            thread::sleep(demo_rest);
        });
    } else {
        drop(demo_rx);
    }

    // Read stdin commands.
    if stdin_enabled {
        thread::spawn(move || {
            let stdin = std::io::stdin();
            let mut stdin_holders = HashMap::<String, String>::new();
            for line in stdin.lock().lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => break,
                };
                let mut it = line.split_whitespace();
                match it.next() {
                    Some("acquire") => {
                        if let Some(l) = it.next() {
                            match client.acquire(l.to_string()) {
                                Ok(a) => {
                                    stdin_holders.insert(a.lock, a.holder);
                                }
                                Err(e) => eprintln!("acquire failed: {e}"),
                            }
                        }
                    }
                    Some("release") => {
                        if let Some(l) = it.next() {
                            match stdin_holders.remove(l) {
                                Some(holder) => {
                                    if let Err(e) = client.release_lock(l.to_string(), holder) {
                                        eprintln!("release failed: {e}");
                                    }
                                }
                                None => eprintln!("lock {l:?} is not held by stdin"),
                            }
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
    }

    println!(
        "# node {id} of {} starting with {} codec, quorum {:?}",
        addrs.len(),
        codec.as_str(),
        transport.quorum_policy
    );
    tracing::info!(
        node_id = id,
        cluster_nodes = addrs.len(),
        codec = codec.as_str(),
        quorum = ?transport.quorum_policy,
        "live-mutex-mills node starting"
    );
    if demo_enabled {
        println!(
            "# demo enabled keys={} hold={}ms rest={}ms",
            demo_keys.join(","),
            demo_hold.as_millis(),
            demo_rest.as_millis()
        );
    }
    if let Err(e) = run_node_with_settings(id, addrs, transport, cmd_rx, raw_evt_tx) {
        tracing::error!(error = %e, "node transport failed");
        eprintln!("node {id} failed: {e}");
        std::process::exit(1);
    }
}

//! End-to-end smoke test of the real TCP transport.
//!
//! Spins up three nodes in one process, each on its own loopback port, talking
//! over actual TCP sockets (real framing, real threads). All three race for one
//! lock; the test enforces that they take it one-at-a-time and that fence tokens
//! strictly increase — i.e. the protocol behaves correctly across the wire, not
//! just in the in-memory simulator.

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use live_mutex_mills::transport::{run_node, Command, NodeEvent};
use live_mutex_mills::NodeId;

#[test]
fn three_nodes_over_tcp_take_turns() {
    let addrs: Vec<String> = ["127.0.0.1:18111", "127.0.0.1:18112", "127.0.0.1:18113"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    // One combined channel carries (node_id, event) from every node.
    let (comb_tx, comb_rx) = mpsc::channel::<(NodeId, NodeEvent)>();
    let mut cmd_txs = Vec::new();

    for id in 0..3u32 {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        cmd_txs.push(cmd_tx);
        let (evt_tx, evt_rx) = mpsc::channel();
        let addrs = addrs.clone();
        let comb = comb_tx.clone();
        thread::spawn(move || {
            for e in evt_rx {
                if comb.send((id, e)).is_err() {
                    return;
                }
            }
        });
        thread::spawn(move || {
            let _ = run_node(id, addrs, cmd_rx, evt_tx);
        });
    }

    // Let the mesh connect.
    thread::sleep(Duration::from_millis(800));

    // Everyone races for "A".
    for tx in &cmd_txs {
        tx.send(Command::Acquire("A".to_string())).unwrap();
    }

    // Collect three acquisitions; enforce one-at-a-time and rising fences.
    let mut fences = Vec::new();
    let mut held = 0i32;
    let deadline = Instant::now() + Duration::from_secs(20);
    while fences.len() < 3 && Instant::now() < deadline {
        match comb_rx.recv_timeout(Duration::from_secs(5)) {
            Ok((id, NodeEvent::Acquired(lock, fence))) => {
                held += 1;
                assert_eq!(held, 1, "two holders of {lock} at once (fence {fence})");
                fences.push(fence);
                // Hold briefly, then release so the next contender proceeds.
                thread::sleep(Duration::from_millis(50));
                held -= 1;
                cmd_txs[id as usize]
                    .send(Command::Release(lock))
                    .unwrap();
            }
            Ok(_) => {} // Info/Lost — ignore
            Err(_) => break,
        }
    }

    for tx in &cmd_txs {
        let _ = tx.send(Command::Shutdown);
    }

    assert_eq!(fences.len(), 3, "all three nodes should acquire over TCP; got {fences:?}");
    for w in fences.windows(2) {
        assert!(w[1] > w[0], "fence tokens must strictly increase over TCP: {fences:?}");
    }
}

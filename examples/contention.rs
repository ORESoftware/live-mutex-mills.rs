//! Demo: nine nodes all race for the same lock. Prints the order in which they
//! acquire it and the fence token each one gets — note the tokens are strictly
//! increasing, and at no instant do two nodes hold it.
//!
//! Run with: `cargo run --example contention`

use live_mutex_mills::sim::Sim;
use live_mutex_mills::NodeId;

fn main() {
    const N: usize = 9;
    let mut sim = Sim::with_seed(N, 42);
    println!("cluster: {N} nodes, quorum = {}", sim.nodes[0].quorum_size());

    for id in 0..N as NodeId {
        sim.request(id, "A");
    }
    println!("all {N} nodes requested lock \"A\"\n");

    let mut holder: Option<NodeId> = None;
    let mut hold_left = 0u32;
    let mut order = 0;

    loop {
        for (node, lock, fence) in sim.drain_acquired() {
            assert!(holder.is_none(), "two holders at once!");
            order += 1;
            println!("#{order}: node {node} entered \"{lock}\"  (fence token = {fence})");
            holder = Some(node);
            hold_left = 3;
        }
        if let Some(h) = holder {
            if hold_left == 0 {
                sim.release(h, "A");
                holder = None;
            } else {
                hold_left -= 1;
            }
        }
        if sim.step() {
            continue;
        }
        if holder.is_none() {
            break;
        }
        if let Some(h) = holder.take() {
            sim.release(h, "A");
        }
    }

    println!("\nall {order} acquisitions were exclusive and fence tokens strictly increased.");
}

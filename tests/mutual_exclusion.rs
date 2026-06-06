//! Property tests: the protocol must never let two requesters hold the same
//! lock at once (safety), every requester must eventually acquire (progress),
//! and a lock's fence tokens must strictly increase across holders.
//!
//! Each test drives many randomized delivery orders through the simulator. A
//! "critical section" is held across several real message deliveries (`HOLD`)
//! so that any erroneous second grant has a window in which to be caught.

use std::collections::HashMap;

use live_mutex_mills::sim::Sim;
use live_mutex_mills::{Fence, NodeId};

/// Hold a lock across this many message deliveries before releasing — long
/// enough to overlap with other contenders' traffic.
const HOLD: u32 = 4;

/// Drive a workload of `(node, lock)` acquire requests to completion, asserting
/// per-lock exclusion, full progress, and strictly increasing fence tokens.
fn run(n: usize, seed: u64, reqs: &[(NodeId, &str)]) {
    let mut sim = Sim::with_seed(n, seed);
    for &(node, lock) in reqs {
        sim.request(node, lock);
    }

    // Per-lock: current holder and deliveries remaining in its critical section.
    let mut holder: HashMap<String, NodeId> = HashMap::new();
    let mut hold_left: HashMap<String, u32> = HashMap::new();
    // Per-lock: acquisition count and the observed token sequence.
    let mut acquired: HashMap<String, u32> = HashMap::new();
    let mut tokens: HashMap<String, Vec<Fence>> = HashMap::new();

    let total = reqs.len();
    let mut steps: u64 = 0;

    loop {
        // 1. Observe new acquisitions; assert exclusion as they appear.
        for (node, lock, fence) in sim.drain_acquired() {
            assert!(
                !holder.contains_key(&lock),
                "MUTUAL EXCLUSION VIOLATED on {lock}: node {} holds it while node {node} acquires (seed {seed})",
                holder[&lock],
            );
            holder.insert(lock.clone(), node);
            hold_left.insert(lock.clone(), HOLD);
            *acquired.entry(lock.clone()).or_default() += 1;
            tokens.entry(lock.clone()).or_default().push(fence);
        }

        // 2. Advance each held critical section; release when its time is up.
        for lock in holder.keys().cloned().collect::<Vec<_>>() {
            let left = hold_left.get_mut(&lock).unwrap();
            if *left == 0 {
                let h = holder.remove(&lock).unwrap();
                hold_left.remove(&lock);
                sim.release(h, &lock);
            } else {
                *left -= 1;
            }
        }

        // 3. Deliver one message.
        if sim.step() {
            steps += 1;
            assert!(steps < 2_000_000, "livelock: no termination (seed {seed})");
            continue;
        }

        // Network idle. Force progress by releasing lingering holders; if there
        // are none, every acquisition has already been observed at step 1, so
        // we're done.
        if holder.is_empty() {
            break;
        }
        for lock in holder.keys().cloned().collect::<Vec<_>>() {
            let h = holder.remove(&lock).unwrap();
            hold_left.remove(&lock);
            sim.release(h, &lock);
        }
    }

    let total_acquired: u32 = acquired.values().sum();
    assert_eq!(
        total_acquired as usize, total,
        "progress: only {total_acquired} of {total} requests acquired (seed {seed})",
    );
    for (lock, seq) in &tokens {
        for w in seq.windows(2) {
            assert!(
                w[1] > w[0],
                "fence tokens not strictly increasing on {lock}: {seq:?} (seed {seed})",
            );
        }
    }
}

#[test]
fn single_lock_9_nodes_many_seeds() {
    let reqs: Vec<(NodeId, &str)> = (0..9).map(|i| (i, "A")).collect();
    for seed in 1..200u64 {
        run(9, seed, &reqs);
    }
}

#[test]
fn odd_cluster_sizes() {
    for &n in &[3usize, 5, 7, 11] {
        let reqs: Vec<(NodeId, &str)> = (0..n as NodeId).map(|i| (i, "A")).collect();
        for seed in 1..60u64 {
            run(n, seed, &reqs);
        }
    }
}

#[test]
fn even_cluster_sizes() {
    // Even n: quorum = n/2 + 1 is still a strict, intersecting majority.
    for &n in &[4usize, 6, 8, 10] {
        let reqs: Vec<(NodeId, &str)> = (0..n as NodeId).map(|i| (i, "A")).collect();
        for seed in 1..60u64 {
            run(n, seed, &reqs);
        }
    }
}

#[test]
fn independent_locks_run_in_parallel() {
    // Half the nodes contend for "A", the other half for "B". Both locks must
    // stay individually exclusive while making full progress — the leaderless
    // property Raft can't match (no single log to serialize through).
    let mut reqs: Vec<(NodeId, &str)> = Vec::new();
    for i in 0..9u32 {
        reqs.push((i, if i % 2 == 0 { "A" } else { "B" }));
    }
    for seed in 1..150u64 {
        run(9, seed, &reqs);
    }
}

#[test]
fn many_nodes_contend_for_many_locks() {
    // Every node wants every one of three locks — heavy preemption traffic.
    let mut reqs: Vec<(NodeId, &str)> = Vec::new();
    for i in 0..7u32 {
        for &l in &["X", "Y", "Z"] {
            reqs.push((i, l));
        }
    }
    for seed in 1..80u64 {
        run(7, seed, &reqs);
    }
}

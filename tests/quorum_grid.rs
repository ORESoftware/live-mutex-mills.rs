//! Tests for the √n grid (true Maekawa) quorum policy.
//!
//! Two halves:
//!   1. The structural guarantee the safety proof rests on — any two grid
//!      quorums intersect — checked exhaustively for several cluster sizes
//!      (perfect squares and partially-filled grids alike).
//!   2. The full mutual-exclusion / progress / fence-monotonicity property test
//!      (the same workload shape as `mutual_exclusion.rs`) driven under
//!      `QuorumPolicy::Grid`, so the smaller quorums are exercised end to end.

use std::collections::{HashMap, HashSet};

use live_mutex_mills::sim::Sim;
use live_mutex_mills::{grid_quorum, Fence, Message, Node, NodeId, QuorumPolicy};

// ---- 1. intersection: the safety-critical structural property --------------

/// For `n` members (ids `0..n`), every pair of grid quorums must share a node.
fn assert_pairwise_intersection(n: usize) {
    let members: Vec<NodeId> = (0..n as NodeId).collect();
    let quorums: Vec<std::collections::HashSet<NodeId>> = members
        .iter()
        .map(|&id| grid_quorum(id, &members).into_iter().collect())
        .collect();
    for i in 0..n {
        // a node is always in its own quorum (it votes for itself)
        assert!(quorums[i].contains(&(i as NodeId)), "n={n}: {i} not in Q_{i}");
        for j in 0..n {
            assert!(
                quorums[i].intersection(&quorums[j]).next().is_some(),
                "n={n}: Q_{i} and Q_{j} do NOT intersect — safety would be violated\n  Q_{i}={:?}\n  Q_{j}={:?}",
                quorums[i],
                quorums[j],
            );
        }
    }
}

#[test]
fn grid_quorums_pairwise_intersect_squares_and_non_squares() {
    // squares (4,9,16,25), one-over-square (5,10,17,26), and awkward sizes.
    for n in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 17, 24, 25, 26, 31, 36] {
        assert_pairwise_intersection(n);
    }
}

#[test]
fn grid_quorum_size_is_about_two_sqrt_n() {
    // For a perfect square n=s^2 the quorum is exactly 2s-1 (a full row + a full
    // column sharing one cell).
    for s in 2..=8usize {
        let n = s * s;
        let members: Vec<NodeId> = (0..n as NodeId).collect();
        let q = grid_quorum(0, &members);
        assert_eq!(q.len(), 2 * s - 1, "n={n}: |Q| should be 2*{s}-1");
        // ... and that's strictly smaller than a majority once s > 1, i.e. fewer
        // messages than the broadcast-to-all majority path.
        assert!(q.len() <= n / 2 + 1, "n={n}: grid quorum not <= majority");
    }
}

// ---- 2. mutual exclusion under Grid policy ---------------------------------

const HOLD: u32 = 4;

/// Same harness as `mutual_exclusion.rs::run`, but parameterized by policy.
fn run_grid(n: usize, seed: u64, reqs: &[(NodeId, &str)]) {
    let mut sim = Sim::with_seed_policy(n, seed, QuorumPolicy::Grid);
    for &(node, lock) in reqs {
        sim.request(node, lock);
    }

    let mut holder: HashMap<String, NodeId> = HashMap::new();
    let mut hold_left: HashMap<String, u32> = HashMap::new();
    let mut acquired: HashMap<String, u32> = HashMap::new();
    let mut tokens: HashMap<String, Vec<Fence>> = HashMap::new();

    let total = reqs.len();
    let mut steps: u64 = 0;

    loop {
        for (node, lock, fence) in sim.drain_acquired() {
            assert!(
                !holder.contains_key(&lock),
                "MUTUAL EXCLUSION VIOLATED on {lock}: node {} holds it while node {node} acquires (grid, seed {seed})",
                holder[&lock],
            );
            holder.insert(lock.clone(), node);
            hold_left.insert(lock.clone(), HOLD);
            *acquired.entry(lock.clone()).or_default() += 1;
            tokens.entry(lock.clone()).or_default().push(fence);
        }

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

        if sim.step() {
            steps += 1;
            assert!(steps < 2_000_000, "livelock: no termination (grid, seed {seed})");
            continue;
        }
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
        "progress: only {total_acquired} of {total} requests acquired (grid, seed {seed})",
    );
    for (lock, seq) in &tokens {
        for w in seq.windows(2) {
            assert!(
                w[1] > w[0],
                "fence tokens not strictly increasing on {lock}: {seq:?} (grid, seed {seed})",
            );
        }
    }
}

#[test]
fn grid_single_lock_all_nodes_contend_many_seeds() {
    // Every node contends for one hot lock; the grid quorum must still serialize
    // them. Run several cluster sizes (squares + non-squares) and many seeds.
    for &n in &[4usize, 5, 9, 10, 16, 17] {
        for seed in 0..40u64 {
            let reqs: Vec<(NodeId, &str)> = (0..n as NodeId).map(|i| (i, "hot")).collect();
            run_grid(n, seed.wrapping_mul(0x9E37_79B9) ^ (n as u64), &reqs);
        }
    }
}

#[test]
fn grid_ignores_grants_from_outside_quorum() {
    // Hardening: the acquire condition is `votes >= threshold`. If a grid node
    // ever counted a grant from a node OUTSIDE its quorum set, threshold could be
    // reached without true quorum coverage — and two non-intersecting vote sets
    // could both acquire. The defensive guard must drop such grants. n=16 so the
    // non-quorum set (9 nodes) is larger than the threshold (7).
    let n = 16usize;
    let members: Vec<NodeId> = (0..n as NodeId).collect();
    let mut node = Node::with_policy(0, members.clone(), QuorumPolicy::Grid);
    let q: HashSet<NodeId> = grid_quorum(0, &members).into_iter().collect();
    let threshold = node.quorum_size();
    assert_eq!(threshold, q.len());

    node.request(0, "L");
    // Recover our own RequestId from the Request messages we just emitted.
    let req = node
        .drain_outbox()
        .into_iter()
        .find_map(|o| match o.msg {
            Message::Request { req, .. } => Some(req),
            _ => None,
        })
        .expect("request() must emit a Request");

    let non_q: Vec<NodeId> = members.iter().copied().filter(|m| !q.contains(m)).collect();
    assert!(non_q.len() >= threshold, "test precondition: |non-quorum| >= threshold");

    // Feed `threshold` grants from NON-quorum nodes — must NOT acquire.
    for &m in non_q.iter().take(threshold) {
        node.handle(0, m, Message::Grant { lock: "L".to_string(), req, fence: 0 });
    }
    assert!(
        node.take_acquired().is_empty(),
        "SAFETY: acquired the lock from out-of-quorum grants — the guard failed",
    );

    // Now feed the real quorum's grants — should acquire exactly once.
    for &m in &q {
        node.handle(0, m, Message::Grant { lock: "L".to_string(), req, fence: 0 });
    }
    assert_eq!(node.take_acquired().len(), 1, "must acquire once the full quorum grants");
}

#[test]
fn grid_independent_locks_run_in_parallel() {
    // Different nodes take different locks — no contention; all should acquire.
    let n = 9;
    for seed in 0..30u64 {
        let mut reqs: Vec<(NodeId, &str)> = Vec::new();
        let locks = ["a", "b", "c"];
        for i in 0..n as NodeId {
            reqs.push((i, locks[(i as usize) % locks.len()]));
        }
        run_grid(n, seed.wrapping_mul(0x1000_0193) ^ 0xABCD, &reqs);
    }
}

//! Holder-failure recovery via vote leases.
//!
//! When a lock holder crashes, the votes it held are stuck until their leases
//! lapse. Once time advances past the lease, arbiters reclaim those votes and
//! hand the lock to a survivor — whose fence token must strictly exceed the
//! dead holder's, so a partitioned-but-alive zombie is fenced out downstream.

use live_mutex_mills::sim::Sim;
use live_mutex_mills::{Fence, NodeId, LEASE};

/// Run the sim until `node` reports acquiring `lock`; returns its fence token.
/// Panics if no acquisition happens within a bounded number of steps.
fn step_until_acquired(sim: &mut Sim, node: NodeId, lock: &str) -> Fence {
    for _ in 0..100_000 {
        for (n, l, fence) in sim.drain_acquired() {
            if n == node && l == lock {
                return fence;
            }
        }
        if !sim.step() {
            break;
        }
    }
    panic!("node {node} never acquired {lock}");
}

#[test]
fn survivor_takes_over_after_holder_crash() {
    // 3 nodes, quorum 2. Node 0 grabs the lock, then dies. Node 1 should take
    // over once node 0's leases lapse — with a strictly greater fence token.
    let mut sim = Sim::with_seed(3, 7);

    sim.request(0, "A");
    let token0 = step_until_acquired(&mut sim, 0, "A");

    // Node 1 wants it too, but can't get a quorum while node 0 holds.
    sim.request(1, "A");
    for _ in 0..1000 {
        if !sim.step() {
            break;
        }
    }
    assert!(
        sim.drain_acquired().is_empty(),
        "node 1 must not acquire while node 0 holds the lock",
    );

    // Node 0 crashes. Its votes are stranded until the lease lapses.
    sim.crash(0);
    sim.advance(LEASE * 2);

    let token1 = step_until_acquired(&mut sim, 1, "A");
    assert!(
        token1 > token0,
        "fence must increase across failover: dead holder {token0}, survivor {token1}",
    );
}

#[test]
fn contended_failover_all_survivors_acquire() {
    // 5 nodes, quorum 3. Everyone wants "A". Whoever wins first gets crashed;
    // the remaining four must still each acquire exactly once, with strictly
    // increasing fence tokens throughout.
    let mut sim = Sim::with_seed(5, 13);
    for id in 0..5u32 {
        sim.request(id, "A");
    }

    // Let the first winner emerge.
    let mut tokens: Vec<Fence> = Vec::new();
    let mut first: Option<NodeId> = None;
    'wait: for _ in 0..100_000 {
        if let Some((n, _l, fence)) = sim.drain_acquired().into_iter().next() {
            first = Some(n);
            tokens.push(fence);
            break 'wait;
        }
        if !sim.step() {
            break;
        }
    }
    let victim = first.expect("someone should have acquired first");

    // Settle so the winner's Confirm reaches its quorum — i.e. its fence token
    // becomes durable — before it dies. (Crashing in the narrow lock→confirm
    // window can reuse a token; see README "token durability".) No one else can
    // acquire while the winner holds, so this produces no new acquisitions.
    while sim.step() {}
    assert!(sim.drain_acquired().is_empty());

    // Crash the holder; the survivors must still each get the lock in turn.
    sim.crash(victim);

    let mut acquirers = std::collections::HashSet::new();
    acquirers.insert(victim);
    for _ in 0..100 {
        // Settle all in-flight delivery.
        while sim.step() {}
        // Record acquisitions and immediately hand the lock on.
        let mut progressed = false;
        for (n, _l, fence) in sim.drain_acquired() {
            acquirers.insert(n);
            tokens.push(fence);
            sim.release(n, "A");
            progressed = true;
        }
        if acquirers.len() == 5 {
            break;
        }
        // No new acquisition this round: a vote is stranded on the dead node
        // (or on its lingering request). Advance time to lapse those leases.
        if !progressed {
            sim.advance(LEASE * 2);
        }
    }

    assert_eq!(
        acquirers.len(),
        5,
        "all 5 nodes (incl. the crashed one) should have held the lock at some point; got {acquirers:?}",
    );
    for w in tokens.windows(2) {
        assert!(
            w[1] > w[0],
            "fence tokens must strictly increase across failover: {tokens:?}",
        );
    }
}

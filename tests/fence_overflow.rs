//! Regression: a fence token at/near u64::MAX must not panic (debug) or wrap to
//! 0 (release). A wrap would make the next holder's token SMALLER than the
//! previous holder's — silently breaking the monotonicity the fence exists to
//! guarantee. The acquire path uses saturating_add(1).

use live_mutex_mills::{Message, Node, NodeId, RequestId};

#[test]
fn fence_saturates_instead_of_wrapping() {
    // Single-node cluster: quorum is 1, the node arbitrates for itself.
    let mut node = Node::new(0, vec![0 as NodeId]);

    // Drive this arbiter's fence_max toward the top of the range via a Confirm
    // carrying a near-max fence (Confirm records the token at the quorum and
    // creates the arbiter entry if absent).
    let poison = RequestId { ts: 1, node: 0 };
    node.handle(
        0,
        0,
        Message::Confirm {
            lock: "L".to_string(),
            req: poison,
            fence: u64::MAX - 1,
        },
    );
    let _ = node.drain_outbox();

    // Now acquire L. on_grant will read fence_max (u64::MAX-1) and compute the
    // chosen token as best_fence.saturating_add(1) == u64::MAX (NOT a wrap to 0,
    // and no overflow panic — reaching this assert at all proves no panic).
    node.request(0, "L");
    // Deliver our own Request -> self-grant -> acquire, by pumping the outbox.
    for _ in 0..8 {
        for out in node.drain_outbox() {
            node.handle(0, out.to, out.msg);
        }
    }

    let acquired = node.take_acquired();
    assert_eq!(acquired.len(), 1, "should have acquired L exactly once");
    assert_eq!(
        acquired[0].1,
        u64::MAX,
        "fence must saturate at u64::MAX, never wrap below the previous token",
    );
}

#[test]
fn idle_prune_preserves_fence_bearing_arbiters() {
    // The tick() memory-prune must drop only fully-idle entries that NEVER
    // recorded a fence. An idle entry with fence_max > 0 must survive — pruning
    // it would reset the fence and let a token be reused.
    let mut node = Node::new(0, vec![0 as NodeId]);

    // Record fence_max = 5 at the arbiter for "L".
    node.handle(
        0,
        0,
        Message::Confirm {
            lock: "L".to_string(),
            req: RequestId { ts: 1, node: 0 },
            fence: 5,
        },
    );
    let _ = node.drain_outbox();

    // Tick across a long idle period — the prune runs every tick.
    for t in 1..6u64 {
        node.tick(t * 10_000);
        let _ = node.drain_outbox();
    }

    // Acquire "L": the (preserved) fence_max=5 must yield token 6, not a reset-to-1.
    node.request(0, "L");
    for _ in 0..8 {
        for out in node.drain_outbox() {
            node.handle(0, out.to, out.msg);
        }
    }
    let acq = node.take_acquired();
    assert_eq!(acq.len(), 1);
    assert_eq!(
        acq[0].1, 6,
        "idle arbiter with fence_max=5 must be preserved (token 6), not pruned and reset to 1",
    );
}

//! A still-acquiring request must be re-broadcast on tick so a `Request` lost
//! while a link was churning (e.g. a k8s rolling restart) self-heals instead of
//! wedging the requester forever.

use live_mutex_mills::{Message, Node, RENEW_INTERVAL};

fn request_count(out: &[live_mutex_mills::Outgoing], lock: &str) -> usize {
    out.iter()
        .filter(|o| matches!(&o.msg, Message::Request { lock: l, .. } if l == lock))
        .count()
}

#[test]
fn pending_request_is_rebroadcast_after_loss() {
    let members = vec![0u32, 1, 2, 3, 4];
    let mut node = Node::new(0, members);

    node.request(0, "A");
    // Simulate the whole initial broadcast being dropped on a churning mesh.
    let initial = node.drain_outbox();
    assert_eq!(request_count(&initial, "A"), 5, "initial broadcast goes to all members");

    // Before the renew interval there is no re-broadcast.
    node.tick(RENEW_INTERVAL - 1);
    assert!(node.drain_outbox().is_empty());

    // After it, the still-acquiring (0 votes < quorum) request is re-sent to all.
    node.tick(RENEW_INTERVAL + 1);
    assert_eq!(
        request_count(&node.drain_outbox(), "A"),
        5,
        "the lost request is re-broadcast to every member",
    );
}

#[test]
fn released_request_is_not_rebroadcast() {
    let members = vec![0u32, 1, 2, 3, 4];
    let mut node = Node::new(0, members);
    node.request(0, "A");
    node.drain_outbox();
    node.release(0, "A");
    node.drain_outbox();

    // Nothing is pending, so a tick produces no Request traffic.
    node.tick(RENEW_INTERVAL + 1);
    assert_eq!(request_count(&node.drain_outbox(), "A"), 0);
}

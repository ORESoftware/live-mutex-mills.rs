use live_mutex_mills::sim::Sim;
use live_mutex_mills::{Message, Node, RequestId, LEASE};

#[test]
fn duplicate_acquire_is_idempotent() {
    let mut n = Node::new(0, vec![0, 1, 2]);
    n.request(0, "A");
    assert_eq!(n.drain_outbox().len(), 3);

    n.request(1, "A");
    assert!(n.drain_outbox().is_empty());
}

#[test]
fn invalid_sender_is_ignored() {
    let mut n = Node::new(0, vec![0, 1, 2]);
    let req = RequestId { ts: 1, node: 1 };

    n.handle(
        0,
        2,
        Message::Request {
            lock: "A".into(),
            req,
        },
    );
    assert!(n.drain_outbox().is_empty());

    n.handle(
        0,
        99,
        Message::Request {
            lock: "A".into(),
            req: RequestId { ts: 1, node: 99 },
        },
    );
    assert!(n.drain_outbox().is_empty());
}

#[test]
fn lease_expiry_revokes_old_votee_and_grants_next() {
    let mut arbiter = Node::new(0, vec![0, 1, 2]);
    let first = RequestId { ts: 1, node: 1 };
    let next = RequestId { ts: 2, node: 2 };

    arbiter.handle(
        0,
        1,
        Message::Request {
            lock: "A".into(),
            req: first,
        },
    );
    let out = arbiter.drain_outbox();
    assert!(matches!(out[0].msg, Message::Grant { .. }));

    arbiter.handle(
        1,
        2,
        Message::Request {
            lock: "A".into(),
            req: next,
        },
    );
    assert!(arbiter.drain_outbox().is_empty());

    arbiter.tick(LEASE + 1);
    let out = arbiter.drain_outbox();
    assert!(out
        .iter()
        .any(|o| o.to == 1 && matches!(o.msg, Message::Revoked { .. })));
    assert!(out
        .iter()
        .any(|o| o.to == 2 && matches!(o.msg, Message::Grant { .. })));
}

#[test]
fn lost_quorum_reports_lost_and_releases_remaining_votes() {
    let mut n = Node::new(0, vec![0, 1, 2]);
    n.request(0, "A");
    let request = n.drain_outbox().remove(0).msg;
    let req = match request {
        Message::Request { req, .. } => req,
        other => panic!("expected request, got {other:?}"),
    };

    n.handle(
        0,
        0,
        Message::Grant {
            lock: "A".into(),
            req,
            fence: 0,
        },
    );
    n.handle(
        0,
        1,
        Message::Grant {
            lock: "A".into(),
            req,
            fence: 0,
        },
    );
    assert_eq!(n.take_acquired(), vec![("A".into(), 1)]);
    n.drain_outbox(); // Confirm messages.

    n.handle(
        0,
        1,
        Message::Revoked {
            lock: "A".into(),
            req,
        },
    );
    assert_eq!(n.take_lost(), vec!["A".to_string()]);
    let releases = n
        .drain_outbox()
        .into_iter()
        .filter(|o| matches!(o.msg, Message::Release { .. }))
        .count();
    assert_eq!(releases, 3);

    n.request(1, "A");
    let requests = n
        .drain_outbox()
        .into_iter()
        .filter(|o| matches!(o.msg, Message::Request { .. }))
        .count();
    assert_eq!(requests, 3);
}

#[test]
#[should_panic(expected = "sim cluster must contain at least one node")]
fn sim_rejects_empty_clusters() {
    let _ = Sim::new(0);
}

#[test]
fn sim_ignores_invalid_node_ids() {
    let mut sim = Sim::new(3);

    sim.request(99, "A");
    sim.release(99, "A");
    sim.crash(99);

    assert!(sim.idle());
    assert!(sim.drain_acquired().is_empty());
}

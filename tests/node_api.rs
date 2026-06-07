use live_mutex_mills::{Fence, Message, Node, NodeId, Outgoing, RequestId, LEASE};

fn request_id(outbox: &[Outgoing]) -> RequestId {
    match outbox.first().map(|out| &out.msg) {
        Some(Message::Request { req, .. }) => *req,
        other => panic!("expected first outgoing message to be a request, got {other:?}"),
    }
}

fn grant(lock: &str, req: RequestId, fence: Fence) -> Message {
    Message::Grant {
        lock: lock.to_string(),
        req,
        fence,
    }
}

fn revoked(lock: &str, req: RequestId) -> Message {
    Message::Revoked {
        lock: lock.to_string(),
        req,
    }
}

fn assert_release_to_all(outbox: &[Outgoing], members: &[NodeId], req: RequestId, fence: Fence) {
    assert_eq!(
        outbox.len(),
        members.len(),
        "release should be broadcast to every member"
    );
    for &member in members {
        assert!(
            outbox.iter().any(|out| {
                out.to == member
                    && matches!(
                        &out.msg,
                        Message::Release {
                            lock,
                            req: got_req,
                            fence: got_fence,
                        } if lock == "A" && *got_req == req && *got_fence == fence
                    )
            }),
            "missing release to node {member}: {outbox:?}",
        );
    }
}

#[test]
#[should_panic(expected = "members must not be empty")]
fn membership_must_not_be_empty() {
    let _ = Node::new(0, vec![]);
}

#[test]
#[should_panic(expected = "members must include the local node id")]
fn membership_must_include_local_node() {
    let _ = Node::new(0, vec![1, 2, 3]);
}

#[test]
#[should_panic(expected = "members must be unique")]
fn membership_must_be_unique() {
    let _ = Node::new(0, vec![0, 1, 1]);
}

#[test]
fn duplicate_request_while_pending_is_idempotent() {
    let mut node = Node::new(0, vec![0, 1, 2]);

    node.request(0, "A");
    let first_outbox = node.drain_outbox();
    assert_eq!(first_outbox.len(), 3);

    node.request(1, "A");
    assert!(
        node.drain_outbox().is_empty(),
        "duplicate acquire should not replace the in-flight request or strand votes",
    );
}

#[test]
fn duplicate_request_while_locked_is_idempotent() {
    let members = vec![0, 1, 2];
    let mut node = Node::new(0, members.clone());

    node.request(0, "A");
    let req = request_id(&node.drain_outbox());
    node.handle(0, 0, grant("A", req, 0));
    node.handle(0, 1, grant("A", req, 0));
    assert_eq!(node.take_acquired(), vec![("A".to_string(), 1)]);
    node.drain_outbox(); // Confirm messages.

    node.request(1, "A");
    assert!(
        node.drain_outbox().is_empty(),
        "duplicate acquire should be a no-op while the lock is held",
    );

    node.release(2, "A");
    let releases = node.drain_outbox();
    assert_release_to_all(&releases, &members, req, 1);
}

#[test]
fn non_member_messages_are_ignored() {
    let mut node = Node::new(0, vec![0, 1, 2]);

    node.request(0, "A");
    let req = request_id(&node.drain_outbox());
    node.handle(0, 99, grant("A", req, 0));
    node.handle(0, 98, grant("A", req, 0));
    assert!(node.take_acquired().is_empty());

    node.handle(0, 1, grant("A", req, 0));
    node.handle(0, 2, grant("A", req, 0));
    assert_eq!(node.take_acquired(), vec![("A".to_string(), 1)]);

    let mut arbiter = Node::new(0, vec![0, 1, 2]);
    let outsider = RequestId { ts: 1, node: 99 };
    arbiter.handle(
        0,
        99,
        Message::Request {
            lock: "A".to_string(),
            req: outsider,
        },
    );
    arbiter.handle(
        0,
        1,
        Message::Request {
            lock: "A".to_string(),
            req: outsider,
        },
    );
    assert!(
        arbiter.drain_outbox().is_empty(),
        "arbiter should not vote for non-members or mismatched request senders",
    );
}

#[test]
fn expired_vote_sends_revoked_before_granting_next_waiter() {
    let mut arbiter = Node::new(2, vec![0, 1, 2]);
    let first = RequestId { ts: 1, node: 0 };
    let second = RequestId { ts: 2, node: 1 };

    arbiter.handle(
        0,
        0,
        Message::Request {
            lock: "A".to_string(),
            req: first,
        },
    );
    assert!(matches!(
        arbiter.drain_outbox().as_slice(),
        [Outgoing {
            to: 0,
            msg: Message::Grant { req, .. },
        }] if *req == first
    ));

    arbiter.handle(
        1,
        1,
        Message::Request {
            lock: "A".to_string(),
            req: second,
        },
    );
    assert!(arbiter.drain_outbox().is_empty());

    arbiter.tick(LEASE);
    let outbox = arbiter.drain_outbox();
    assert!(
        outbox
            .iter()
            .any(|out| out.to == 0
                && matches!(&out.msg, Message::Revoked { req, .. } if *req == first)),
        "expired votee should be told its vote was reclaimed: {outbox:?}",
    );
    assert!(
        outbox
            .iter()
            .any(|out| out.to == 1
                && matches!(&out.msg, Message::Grant { req, .. } if *req == second)),
        "next waiter should receive the reclaimed vote: {outbox:?}",
    );
}

#[test]
fn lost_holder_releases_remaining_votes_and_can_request_again() {
    let members = vec![0, 1, 2];
    let mut node = Node::new(0, members.clone());

    node.request(0, "A");
    let req = request_id(&node.drain_outbox());
    node.handle(0, 0, grant("A", req, 0));
    node.handle(0, 1, grant("A", req, 0));
    assert_eq!(node.take_acquired(), vec![("A".to_string(), 1)]);
    node.drain_outbox(); // Confirm messages.

    node.handle(LEASE, 1, revoked("A", req));
    assert_eq!(node.take_lost(), vec!["A".to_string()]);
    let cleanup = node.drain_outbox();
    assert_release_to_all(&cleanup, &members, req, 1);

    node.request(LEASE + 1, "A");
    let retry = node.drain_outbox();
    assert_eq!(retry.len(), 3);
    assert!(
        request_id(&retry) > req,
        "retry should create a fresh request id after the old held lock was lost",
    );
}

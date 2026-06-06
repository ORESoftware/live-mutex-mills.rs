//! # live-mutex-mills
//!
//! A leaderless, **quorum/vote** distributed mutual-exclusion protocol — an
//! alternative to Raft/Paxos specialized for *locking* rather than general
//! state-machine replication.
//!
//! ## Why this exists
//!
//! Raft/Paxos funnel every operation through a single leader's log, even when
//! the operations are independent. For a lock service that's wasteful: lock `A`
//! and lock `B` have nothing to do with each other. This protocol is
//! **leaderless** — any node can serve any request — so independent locks run
//! fully in parallel and there is no leader election to stall on failure.
//!
//! ## The three mechanisms (and what each one is *for*)
//!
//! 1. **Quorum voting → SAFETY.** To hold a lock you must collect votes from a
//!    quorum (a strict majority) of nodes, and each node grants its vote to at
//!    most one request per lock at a time. Two holders would need two quorums,
//!    which must intersect (`q + q > n`), so some node granted twice — a
//!    contradiction. This argument uses **no timing assumptions**: it is safe
//!    in a fully asynchronous network (arbitrary delays, skew, GC pauses),
//!    exactly like Raft/Paxos and *unlike* a clock/commit-wait design.
//!
//! 2. **Lamport timestamps + INQUIRE/YIELD preemption → DEADLOCK-FREEDOM.**
//!    Without ordering, votes can split 3/3/3 and nobody reaches a majority.
//!    Each request carries a totally-ordered [`RequestId`] = `(lamport, node)`.
//!    When a node has voted for `a` and a *smaller* request `b` arrives, it
//!    asks `a` to yield. A holder that has not yet locked yields; one that has
//!    locked keeps its votes until it releases. The globally-smallest request
//!    can never be preempted, so it always makes progress — no deadlock, no
//!    starvation. These timestamps are **logical**, so no physical clock is
//!    needed anywhere on the happy path.
//!
//! 3. **Monotonic per-lock fence tokens → DOWNSTREAM BACKSTOP.** Every grant
//!    carries the granter's highest seen token; on locking, the holder takes
//!    `max(seen) + 1` and propagates it. Because the next holder needs at least
//!    one vote from the previous holder's quorum (intersection again), its
//!    token is strictly greater. A client presents this token to the protected
//!    resource, which rejects any operation with a stale token — so even a
//!    partitioned-but-alive stale holder cannot corrupt state.
//!
//! ## What's implemented here (v1)
//!
//! The transport-agnostic protocol state machine ([`Node`]) and an in-memory
//! network [`sim`]ulator used to assert mutual exclusion under randomized
//! delivery. A [`Node`] never does I/O: you feed it messages with
//! [`Node::handle`] and drain what it wants to send with
//! [`Node::drain_outbox`]. See `README.md` for the roadmap (vote leases for
//! holder failure, real networking, √n quorums).

use std::collections::{BTreeSet, HashMap, HashSet};

pub mod sim;

/// Identifier of a node in the cluster.
pub type NodeId = u32;
/// Name of a lock (the resource under contention).
pub type LockId = String;
/// A Lamport logical-clock value.
pub type Lamport = u64;
/// A fence token: strictly increases across successive holders of one lock.
pub type Fence = u64;

/// A globally unique, totally ordered request identifier.
///
/// Ordering is `(ts, node)` lexicographically — derived `Ord` compares fields
/// in declaration order, so a smaller Lamport timestamp wins and the node id
/// breaks ties. This total order is what makes the preemption protocol
/// deadlock- and starvation-free.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RequestId {
    pub ts: Lamport,
    pub node: NodeId,
}

/// A protocol message. Every message names the lock it concerns; the *sender*
/// is supplied out-of-band to [`Node::handle`] (it is `req.node` for a
/// `Request`, and the arbiter's id for a `Grant`/`Inquire`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Requester → all arbiters: "please vote for me."
    Request { lock: LockId, req: RequestId },
    /// Arbiter → requester: "you have my vote." Carries the arbiter's highest
    /// fence token so the requester can compute a strictly-greater one.
    Grant { lock: LockId, req: RequestId, fence: Fence },
    /// Arbiter → current votee: "someone older wants this; will you yield?"
    Inquire { lock: LockId, req: RequestId },
    /// Requester → arbiter: "taking my vote back, re-grant it." Sent only by a
    /// requester that has *not* yet locked.
    Yield { lock: LockId, req: RequestId },
    /// Requester → quorum: records the chosen fence token at the quorum so it
    /// survives the holder crashing before [`Message::Release`].
    Confirm { lock: LockId, req: RequestId, fence: Fence },
    /// Requester → arbiters: "done, free your vote." Carries the token so the
    /// arbiter bumps its fence *before* granting the next holder.
    Release { lock: LockId, req: RequestId, fence: Fence },
}

/// An outgoing message addressed to a node. The transport (or [`sim`]) decides
/// how/when to deliver it; the protocol makes no timing assumptions.
#[derive(Clone, Debug)]
pub struct Outgoing {
    pub to: NodeId,
    pub msg: Message,
}

/// Per-lock state when this node acts as an **arbiter** (a voter).
#[derive(Debug, Default)]
struct ArbiterState {
    /// The request this node has currently granted its vote to.
    voted_for: Option<RequestId>,
    /// Whether an INQUIRE is already outstanding for the current votee (so we
    /// don't inquire repeatedly).
    inquired: bool,
    /// Waiting requests, ordered so the smallest [`RequestId`] is first. A
    /// `BTreeSet` (not a heap) so a `Release` can evict a request from the
    /// middle of the queue — see [`Node::on_release`].
    queue: BTreeSet<RequestId>,
    /// Highest fence token this arbiter has acknowledged for the lock.
    fence_max: Fence,
}

/// Per-lock state when this node acts as a **requester** (wants the lock).
#[derive(Debug)]
struct RequesterState {
    req: RequestId,
    /// Arbiters that have granted their vote to `req`.
    votes: HashSet<NodeId>,
    /// While unlocked: the max fence seen from granters. Once locked: the
    /// chosen token (`max + 1`).
    best_fence: Fence,
    locked: bool,
}

/// A single node's protocol state machine. Pure logic — no I/O, no clocks.
///
/// Drive it by calling [`Node::request`]/[`Node::release`] to express intent,
/// [`Node::handle`] to feed it an inbound message, then [`Node::drain_outbox`]
/// to collect what it wants to send. [`Node::take_acquired`] reports locks the
/// node has just entered (with their fence token).
pub struct Node {
    pub id: NodeId,
    members: Vec<NodeId>,
    quorum: usize,
    lamport: Lamport,
    arbiter: HashMap<LockId, ArbiterState>,
    requester: HashMap<LockId, RequesterState>,
    outbox: Vec<Outgoing>,
    acquired: Vec<(LockId, Fence)>,
}

impl Node {
    /// Create a node. `members` is the full cluster membership (including
    /// `id`). The quorum is a strict majority: `floor(n/2) + 1`.
    pub fn new(id: NodeId, members: Vec<NodeId>) -> Self {
        let quorum = members.len() / 2 + 1;
        Node {
            id,
            members,
            quorum,
            lamport: 0,
            arbiter: HashMap::new(),
            requester: HashMap::new(),
            outbox: Vec::new(),
            acquired: Vec::new(),
        }
    }

    /// The quorum size (`floor(n/2) + 1`).
    pub fn quorum_size(&self) -> usize {
        self.quorum
    }

    /// Begin acquiring `lock`. Broadcasts a vote request to every member
    /// (including self). The lock is held once [`Node::take_acquired`] reports
    /// it. Re-requesting a lock already being acquired/held replaces the
    /// in-flight request.
    pub fn request(&mut self, lock: &str) {
        let ts = self.tick();
        let req = RequestId { ts, node: self.id };
        self.requester.insert(
            lock.to_string(),
            RequesterState {
                req,
                votes: HashSet::new(),
                best_fence: 0,
                locked: false,
            },
        );
        for m in self.members.clone() {
            self.send(m, Message::Request { lock: lock.to_string(), req });
        }
    }

    /// Release a held (or in-flight) lock, freeing votes at every arbiter that
    /// granted to it. The current fence token rides along so arbiters record it
    /// before handing the lock to the next holder.
    pub fn release(&mut self, lock: &str) {
        if let Some(state) = self.requester.remove(lock) {
            let fence = state.best_fence;
            for m in self.members.clone() {
                self.send(
                    m,
                    Message::Release { lock: lock.to_string(), req: state.req, fence },
                );
            }
        }
    }

    /// Feed an inbound message. `from` is the node that sent it.
    pub fn handle(&mut self, from: NodeId, msg: Message) {
        match msg {
            Message::Request { lock, req } => self.on_request(lock, req),
            Message::Grant { lock, req, fence } => self.on_grant(from, lock, req, fence),
            Message::Inquire { lock, req } => self.on_inquire(from, lock, req),
            Message::Yield { lock, req } => self.on_yield(lock, req),
            Message::Confirm { lock, req, fence } => self.on_confirm(lock, req, fence),
            Message::Release { lock, req, fence } => self.on_release(lock, req, fence),
        }
    }

    /// Take everything the node wants to send since the last drain.
    pub fn drain_outbox(&mut self) -> Vec<Outgoing> {
        std::mem::take(&mut self.outbox)
    }

    /// Take the locks newly acquired (entered) since the last call, each paired
    /// with its fence token. Present the token to the protected resource.
    pub fn take_acquired(&mut self) -> Vec<(LockId, Fence)> {
        std::mem::take(&mut self.acquired)
    }

    // ---- internals -------------------------------------------------------

    fn tick(&mut self) -> Lamport {
        self.lamport += 1;
        self.lamport
    }

    fn observe(&mut self, ts: Lamport) {
        if ts > self.lamport {
            self.lamport = ts;
        }
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Outgoing { to, msg });
    }

    // ---- arbiter role ----------------------------------------------------

    fn on_request(&mut self, lock: LockId, req: RequestId) {
        self.observe(req.ts);
        let mut grant: Option<(NodeId, Fence)> = None;
        let mut inquire: Option<RequestId> = None;
        {
            let a = self.arbiter.entry(lock.clone()).or_default();
            match a.voted_for {
                None => {
                    // Free: grant immediately to the requester.
                    a.voted_for = Some(req);
                    grant = Some((req.node, a.fence_max));
                }
                Some(current) => {
                    if current == req {
                        return; // duplicate of the current votee
                    }
                    a.queue.insert(req);
                    // A strictly-smaller request should preempt the votee.
                    if req < current && !a.inquired {
                        a.inquired = true;
                        inquire = Some(current);
                    }
                }
            }
        }
        if let Some((to, fence)) = grant {
            self.send(to, Message::Grant { lock: lock.clone(), req, fence });
        }
        if let Some(current) = inquire {
            self.send(current.node, Message::Inquire { lock, req: current });
        }
    }

    /// Grant the vote to the smallest waiting request, if any. Updates
    /// `voted_for` (to the chosen request, or `None` if the queue is empty) and
    /// returns the `Grant` to send.
    fn grant_next(a: &mut ArbiterState) -> Option<(NodeId, RequestId, Fence)> {
        match a.queue.iter().next().copied() {
            Some(next) => {
                a.queue.remove(&next);
                a.voted_for = Some(next);
                Some((next.node, next, a.fence_max))
            }
            None => {
                a.voted_for = None;
                None
            }
        }
    }

    fn on_yield(&mut self, lock: LockId, req: RequestId) {
        let mut grant: Option<(NodeId, RequestId, Fence)> = None;
        if let Some(a) = self.arbiter.get_mut(&lock) {
            if a.voted_for != Some(req) {
                return; // stale yield (we already moved on)
            }
            // The yielder rejoins the queue; grant to whoever is now smallest.
            a.queue.insert(req);
            a.inquired = false;
            grant = Self::grant_next(a);
        }
        if let Some((to, r, fence)) = grant {
            self.send(to, Message::Grant { lock, req: r, fence });
        }
    }

    fn on_confirm(&mut self, lock: LockId, _req: RequestId, fence: Fence) {
        let a = self.arbiter.entry(lock).or_default();
        if fence > a.fence_max {
            a.fence_max = fence;
        }
    }

    fn on_release(&mut self, lock: LockId, req: RequestId, fence: Fence) {
        let mut grant: Option<(NodeId, RequestId, Fence)> = None;
        if let Some(a) = self.arbiter.get_mut(&lock) {
            // Record the departing holder's token BEFORE granting next, so the
            // next holder's grant carries a strictly-greater fence.
            if fence > a.fence_max {
                a.fence_max = fence;
            }
            // Evict the releasing request wherever it sits. Crucially this
            // includes the queue: a request can acquire/release via a quorum
            // that excluded this arbiter while still lingering in its queue
            // (it yielded this vote earlier). Without eviction we'd later grant
            // our vote to that dead request and stick forever.
            a.queue.remove(&req);
            if a.voted_for != Some(req) {
                return; // not the current votee; queue eviction was enough
            }
            a.inquired = false;
            grant = Self::grant_next(a);
        }
        if let Some((to, r, fence)) = grant {
            self.send(to, Message::Grant { lock, req: r, fence });
        }
    }

    // ---- requester role --------------------------------------------------

    fn on_grant(&mut self, from: NodeId, lock: LockId, req: RequestId, fence: Fence) {
        let quorum = self.quorum;
        let mut confirm: Vec<NodeId> = Vec::new();
        let mut token: Fence = 0;
        let mut newly_acquired: Option<Fence> = None;
        if let Some(r) = self.requester.get_mut(&lock) {
            if r.req != req {
                return; // grant for a superseded request
            }
            if r.locked {
                // Late grant after we already locked: count the voter (so we
                // release its vote later) and record our token at it too.
                r.votes.insert(from);
                token = r.best_fence;
                confirm.push(from);
            } else {
                r.votes.insert(from);
                if fence > r.best_fence {
                    r.best_fence = fence;
                }
                if r.votes.len() >= quorum {
                    // Quorum reached — enter the critical section.
                    r.locked = true;
                    let chosen = r.best_fence + 1;
                    r.best_fence = chosen;
                    token = chosen;
                    newly_acquired = Some(chosen);
                    confirm = r.votes.iter().copied().collect();
                }
            }
        } else {
            return;
        }
        if let Some(t) = newly_acquired {
            self.acquired.push((lock.clone(), t));
        }
        for to in confirm {
            self.send(to, Message::Confirm { lock: lock.clone(), req, fence: token });
        }
    }

    fn on_inquire(&mut self, from: NodeId, lock: LockId, req: RequestId) {
        let mut do_yield = false;
        if let Some(r) = self.requester.get_mut(&lock) {
            if r.req != req {
                return;
            }
            // Yield only if we have NOT yet locked. A holder keeps its votes
            // until it releases — that's what makes the globally-smallest
            // request always able to win, and keeps exclusion intact.
            if !r.locked {
                r.votes.remove(&from);
                do_yield = true;
            }
        }
        if do_yield {
            self.send(from, Message::Yield { lock, req });
        }
    }
}

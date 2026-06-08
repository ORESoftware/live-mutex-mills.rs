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

use serde::{Deserialize, Serialize};

pub mod broker_runtime_config;
pub mod client_api;
pub mod codec;
pub mod composite;
pub mod sim;
pub mod transport;

/// Identifier of a node in the cluster.
pub type NodeId = u32;
/// Name of a lock (the resource under contention).
pub type LockId = String;
/// A Lamport logical-clock value.
pub type Lamport = u64;
/// A fence token: strictly increases across successive holders of one lock.
pub type Fence = u64;
/// A point in physical time, in arbitrary monotonic units (e.g. milliseconds).
/// The protocol never reads a clock itself — callers pass `now` in. Time is
/// used *only* for lease expiry on the failure path; the happy path is
/// clock-free.
pub type Instant = u64;

/// How long a granted vote stays valid without a renewal. If a holder stops
/// renewing (crash/partition) its votes are reclaimed after this long.
pub const LEASE: Instant = 10_000;
/// How often a requester renews the votes it currently holds. Must be
/// comfortably smaller than [`LEASE`] so a few lost renewals are survivable.
pub const RENEW_INTERVAL: Instant = 2_000;

/// A globally unique, totally ordered request identifier.
///
/// Ordering is `(ts, node)` lexicographically — derived `Ord` compares fields
/// in declaration order, so a smaller Lamport timestamp wins and the node id
/// breaks ties. This total order is what makes the preemption protocol
/// deadlock- and starvation-free.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct RequestId {
    pub ts: Lamport,
    pub node: NodeId,
}

/// A protocol message. Every message names the lock it concerns; the *sender*
/// is supplied out-of-band to [`Node::handle`]. Requester-to-arbiter messages
/// must come from `req.node`; arbiter-to-requester messages must be addressed
/// to the local node's request id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Requester → all arbiters: "please vote for me."
    Request { lock: LockId, req: RequestId },
    /// Arbiter → requester: "you have my vote." Carries the arbiter's highest
    /// fence token so the requester can compute a strictly-greater one.
    Grant {
        lock: LockId,
        req: RequestId,
        fence: Fence,
    },
    /// Arbiter → current votee: "someone older wants this; will you yield?"
    Inquire { lock: LockId, req: RequestId },
    /// Requester → arbiter: "taking my vote back, re-grant it." Sent only by a
    /// requester that has *not* yet locked.
    Yield { lock: LockId, req: RequestId },
    /// Requester → quorum: records the chosen fence token at the quorum so it
    /// survives the holder crashing before [`Message::Release`].
    Confirm {
        lock: LockId,
        req: RequestId,
        fence: Fence,
    },
    /// Requester → arbiters: "done, free your vote." Carries the token so the
    /// arbiter bumps its fence *before* granting the next holder.
    Release {
        lock: LockId,
        req: RequestId,
        fence: Fence,
    },
    /// Requester → arbiters: "I'm still alive, extend my vote's lease." Sent
    /// periodically while a requester holds (or is collecting) votes.
    Renew { lock: LockId, req: RequestId },
    /// Arbiter → requester: "your lease lapsed and I've given the vote away."
    /// Lets a slow-but-alive holder learn it has lost the lock so it can stop
    /// using the resource (the fence token is the hard backstop regardless).
    Revoked { lock: LockId, req: RequestId },
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
    /// Deadline by which the current votee must renew, else its vote is
    /// reclaimed. Meaningful only while `voted_for` is `Some`.
    lease: Instant,
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
    /// Next time this requester should renew the votes it holds.
    next_renew: Instant,
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
    lost: Vec<LockId>,
    /// Most recent time fed in by the caller (via `handle`/`tick`/etc.).
    now: Instant,
}

impl Node {
    /// Create a node. `members` is the full cluster membership (including
    /// `id`) with no duplicates. The quorum is a strict majority:
    /// `floor(n/2) + 1`.
    pub fn new(id: NodeId, members: Vec<NodeId>) -> Self {
        assert!(!members.is_empty(), "members must not be empty");
        assert!(
            members.contains(&id),
            "members must include the local node id"
        );
        let unique: HashSet<NodeId> = members.iter().copied().collect();
        assert_eq!(unique.len(), members.len(), "members must be unique");
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
            lost: Vec::new(),
            now: 0,
        }
    }

    /// The quorum size (`floor(n/2) + 1`).
    pub fn quorum_size(&self) -> usize {
        self.quorum
    }

    /// Begin acquiring `lock`. Broadcasts a vote request to every member
    /// (including self). The lock is held once [`Node::take_acquired`] reports
    /// it. Re-requesting a lock already being acquired/held is idempotent.
    pub fn request(&mut self, now: Instant, lock: &str) {
        self.now = now;
        if self.requester.contains_key(lock) {
            return;
        }
        let ts = self.lamport_tick();
        let req = RequestId { ts, node: self.id };
        self.requester.insert(
            lock.to_string(),
            RequesterState {
                req,
                votes: HashSet::new(),
                best_fence: 0,
                locked: false,
                next_renew: now + RENEW_INTERVAL,
            },
        );
        for m in self.members.clone() {
            self.send(
                m,
                Message::Request {
                    lock: lock.to_string(),
                    req,
                },
            );
        }
    }

    /// Release a held (or in-flight) lock, freeing votes at every arbiter that
    /// granted to it. The current fence token rides along so arbiters record it
    /// before handing the lock to the next holder.
    pub fn release(&mut self, now: Instant, lock: &str) {
        self.now = now;
        if let Some(state) = self.requester.remove(lock) {
            let fence = state.best_fence;
            for m in self.members.clone() {
                self.send(
                    m,
                    Message::Release {
                        lock: lock.to_string(),
                        req: state.req,
                        fence,
                    },
                );
            }
        }
    }

    /// Feed an inbound message. `from` is the node that sent it; `now` is the
    /// caller's current time.
    pub fn handle(&mut self, now: Instant, from: NodeId, msg: Message) {
        self.now = now;
        if !self.valid_inbound(from, &msg) {
            return;
        }
        match msg {
            Message::Request { lock, req } => self.on_request(lock, req),
            Message::Grant { lock, req, fence } => self.on_grant(from, lock, req, fence),
            Message::Inquire { lock, req } => self.on_inquire(from, lock, req),
            Message::Yield { lock, req } => self.on_yield(lock, req),
            Message::Confirm { lock, req, fence } => self.on_confirm(lock, req, fence),
            Message::Release { lock, req, fence } => self.on_release(lock, req, fence),
            Message::Renew { lock, req } => self.on_renew(lock, req),
            Message::Revoked { lock, req } => self.on_revoked(from, lock, req),
        }
    }

    /// Advance time. Drives lease maintenance: reclaims votes whose holders
    /// stopped renewing, and emits renewals for votes this node still holds.
    /// Call periodically (the transport ties this to a wall clock).
    pub fn tick(&mut self, now: Instant) {
        self.now = now;

        // Arbiter role: reclaim any votee whose lease has lapsed.
        let mut revoked: Vec<(NodeId, LockId, RequestId)> = Vec::new();
        let mut grants: Vec<(LockId, (NodeId, RequestId, Fence))> = Vec::new();
        for (lock, a) in self.arbiter.iter_mut() {
            if a.voted_for.is_some() && now >= a.lease {
                if let Some(expired) = a.voted_for {
                    revoked.push((expired.node, lock.clone(), expired));
                }
                if let Some(g) = Self::grant_next(a, now) {
                    grants.push((lock.clone(), g));
                }
            }
        }
        for (to, lock, req) in revoked {
            self.send(to, Message::Revoked { lock, req });
        }
        for (lock, (to, r, fence)) in grants {
            self.send(
                to,
                Message::Grant {
                    lock,
                    req: r,
                    fence,
                },
            );
        }

        // Requester role: renew held votes, and re-broadcast any request that is
        // still acquiring. Re-broadcasting recovers a Request lost while a link
        // was churning (e.g. a rolling restart): receivers de-duplicate it and
        // the RequestId is unchanged, so queue fairness is preserved. (A lost
        // Grant self-heals separately — the arbiter's vote lease lapses and it
        // re-grants.) Without this a one-shot message loss wedges the requester.
        let quorum = self.quorum;
        let mut renews: Vec<(NodeId, LockId, RequestId)> = Vec::new();
        let mut rebroadcasts: Vec<(LockId, RequestId)> = Vec::new();
        for (lock, r) in self.requester.iter_mut() {
            if now >= r.next_renew {
                r.next_renew = now + RENEW_INTERVAL;
                for &v in r.votes.iter() {
                    renews.push((v, lock.clone(), r.req));
                }
                if !r.locked && r.votes.len() < quorum {
                    rebroadcasts.push((lock.clone(), r.req));
                }
            }
        }
        for (to, lock, req) in renews {
            self.send(to, Message::Renew { lock, req });
        }
        if !rebroadcasts.is_empty() {
            let members = self.members.clone();
            for (lock, req) in rebroadcasts {
                for &m in &members {
                    self.send(m, Message::Request { lock: lock.clone(), req });
                }
            }
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

    /// Take the locks this node has *lost* since the last call (its lease
    /// lapsed and another holder took over). The application must stop using
    /// the resource for these locks.
    pub fn take_lost(&mut self) -> Vec<LockId> {
        std::mem::take(&mut self.lost)
    }

    // ---- internals -------------------------------------------------------

    fn lamport_tick(&mut self) -> Lamport {
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

    fn is_member(&self, id: NodeId) -> bool {
        self.members.contains(&id)
    }

    fn valid_inbound(&self, from: NodeId, msg: &Message) -> bool {
        if !self.is_member(from) {
            return false;
        }

        let req = match msg {
            Message::Request { req, .. }
            | Message::Grant { req, .. }
            | Message::Inquire { req, .. }
            | Message::Yield { req, .. }
            | Message::Confirm { req, .. }
            | Message::Release { req, .. }
            | Message::Renew { req, .. }
            | Message::Revoked { req, .. } => req,
        };
        if !self.is_member(req.node) {
            return false;
        }

        match msg {
            Message::Request { .. }
            | Message::Yield { .. }
            | Message::Confirm { .. }
            | Message::Release { .. }
            | Message::Renew { .. } => from == req.node,
            Message::Grant { .. } | Message::Inquire { .. } | Message::Revoked { .. } => {
                req.node == self.id
            }
        }
    }

    // ---- arbiter role ----------------------------------------------------

    fn on_request(&mut self, lock: LockId, req: RequestId) {
        self.observe(req.ts);
        let now = self.now;
        let mut grant: Option<(NodeId, Fence)> = None;
        let mut inquire: Option<RequestId> = None;
        {
            let a = self.arbiter.entry(lock.clone()).or_default();
            match a.voted_for {
                None => {
                    // Free: grant immediately to the requester.
                    a.voted_for = Some(req);
                    a.lease = now + LEASE;
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
            self.send(
                to,
                Message::Grant {
                    lock: lock.clone(),
                    req,
                    fence,
                },
            );
        }
        if let Some(current) = inquire {
            self.send(current.node, Message::Inquire { lock, req: current });
        }
    }

    /// Grant the vote to the smallest waiting request, if any. Updates
    /// `voted_for` (to the chosen request, or `None` if the queue is empty),
    /// arms a fresh lease, and returns the `Grant` to send.
    fn grant_next(a: &mut ArbiterState, now: Instant) -> Option<(NodeId, RequestId, Fence)> {
        a.inquired = false;
        match a.queue.iter().next().copied() {
            Some(next) => {
                a.queue.remove(&next);
                a.voted_for = Some(next);
                a.lease = now + LEASE;
                Some((next.node, next, a.fence_max))
            }
            None => {
                a.voted_for = None;
                None
            }
        }
    }

    fn on_yield(&mut self, lock: LockId, req: RequestId) {
        let now = self.now;
        let mut grant: Option<(NodeId, RequestId, Fence)> = None;
        if let Some(a) = self.arbiter.get_mut(&lock) {
            if a.voted_for != Some(req) {
                return; // stale yield (we already moved on)
            }
            // The yielder rejoins the queue; grant to whoever is now smallest.
            a.queue.insert(req);
            a.inquired = false;
            grant = Self::grant_next(a, now);
        }
        if let Some((to, r, fence)) = grant {
            self.send(
                to,
                Message::Grant {
                    lock,
                    req: r,
                    fence,
                },
            );
        }
    }

    fn on_confirm(&mut self, lock: LockId, _req: RequestId, fence: Fence) {
        let a = self.arbiter.entry(lock).or_default();
        if fence > a.fence_max {
            a.fence_max = fence;
        }
    }

    fn on_release(&mut self, lock: LockId, req: RequestId, fence: Fence) {
        let now = self.now;
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
            grant = Self::grant_next(a, now);
        }
        if let Some((to, r, fence)) = grant {
            self.send(
                to,
                Message::Grant {
                    lock,
                    req: r,
                    fence,
                },
            );
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
            self.send(
                to,
                Message::Confirm {
                    lock: lock.clone(),
                    req,
                    fence: token,
                },
            );
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

    // ---- lease maintenance ----------------------------------------------

    /// Arbiter: extend the current votee's lease, or tell a stale requester it
    /// has lost the vote.
    fn on_renew(&mut self, lock: LockId, req: RequestId) {
        let now = self.now;
        let mut revoke = false;
        if let Some(a) = self.arbiter.get_mut(&lock) {
            if a.voted_for == Some(req) {
                a.lease = now + LEASE;
            } else {
                revoke = true;
            }
        } else {
            revoke = true;
        }
        if revoke {
            self.send(req.node, Message::Revoked { lock, req });
        }
    }

    /// Requester: an arbiter reclaimed our vote. Drop it; if that drops us below
    /// quorum while we believed we held the lock, we have lost it.
    fn on_revoked(&mut self, from: NodeId, lock: LockId, req: RequestId) {
        let quorum = self.quorum;
        let mut lost = false;
        if let Some(r) = self.requester.get_mut(&lock) {
            if r.req != req {
                return;
            }
            r.votes.remove(&from);
            if r.locked && r.votes.len() < quorum {
                lost = true;
            }
        }
        if lost {
            let state = self
                .requester
                .remove(&lock)
                .expect("requester state existed when loss was detected");
            let fence = state.best_fence;
            for m in self.members.clone() {
                self.send(
                    m,
                    Message::Release {
                        lock: lock.clone(),
                        req: state.req,
                        fence,
                    },
                );
            }
            self.lost.push(lock);
        }
    }
}

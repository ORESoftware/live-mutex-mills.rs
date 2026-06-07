//! A deterministic in-memory network simulator for driving and testing the
//! protocol without sockets.
//!
//! ## Channel model: FIFO per directed link (i.e. TCP)
//!
//! This protocol — like Maekawa's and most quorum mutual-exclusion algorithms
//! — assumes **FIFO point-to-point channels**: messages between an ordered pair
//! of nodes are delivered in send order. That is precisely what a single TCP
//! connection provides, and it is *required* for correctness. Concretely:
//! arbiter `X` may send `Grant→A` and then `Inquire→A`; if those were
//! reordered, `A` could re-count a vote `X` has since handed to someone else,
//! and two requesters could both reach a quorum. With FIFO links that can't
//! happen.
//!
//! So the simulator keeps an independent queue per directed link and, on each
//! step, advances a *randomly chosen* link by delivering its head. This
//! preserves per-link order (TCP) while interleaving links and locks
//! arbitrarily — which is where the real concurrency and races live. Delivery
//! choice is driven by a seedable xorshift PRNG so every run is reproducible.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::{Fence, Instant, LockId, Message, Node, NodeId, Outgoing, RENEW_INTERVAL};

/// An in-memory cluster of [`Node`]s connected by FIFO (TCP-like) links.
///
/// Node ids are `0..n` and equal their index in [`Sim::nodes`]. A logical
/// `clock` is held fixed during message delivery ([`Sim::step`]) and only moves
/// when the test passes time explicitly ([`Sim::advance`]) — so lease behaviour
/// is exercised deliberately rather than incidentally.
pub struct Sim {
    pub nodes: Vec<Node>,
    /// One FIFO queue per directed `(from, to)` link.
    links: HashMap<(NodeId, NodeId), VecDeque<Message>>,
    /// Crashed nodes: they neither send, receive, nor tick.
    dead: HashSet<NodeId>,
    clock: Instant,
    rng: u64,
}

impl Sim {
    /// Build an `n`-node cluster with a default seed.
    pub fn new(n: usize) -> Self {
        Self::with_seed(n, 0x9E37_79B9_7F4A_7C15)
    }

    /// Build an `n`-node cluster with an explicit PRNG seed (for reproducible
    /// delivery orders across test cases).
    pub fn with_seed(n: usize, seed: u64) -> Self {
        assert!(n > 0, "sim cluster must contain at least one node");
        let members: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = (0..n as NodeId)
            .map(|id| Node::new(id, members.clone()))
            .collect();
        Sim {
            nodes,
            links: HashMap::new(),
            dead: HashSet::new(),
            clock: 0,
            rng: if seed == 0 {
                0x1234_5678_9ABC_DEF1
            } else {
                seed
            },
        }
    }

    /// The current logical time.
    pub fn now(&self) -> Instant {
        self.clock
    }

    /// Crash `node`: it stops sending, receiving, and ticking, and all messages
    /// queued to/from it are dropped (a fail-stop + partition). Its votes held
    /// at other arbiters will lapse and be reclaimed once time advances.
    pub fn crash(&mut self, node: NodeId) {
        if !self.has_node(node) {
            return;
        }
        self.dead.insert(node);
        self.links
            .retain(|&(from, to), _| from != node && to != node);
    }

    /// Have `node` start acquiring `lock`.
    pub fn request(&mut self, node: NodeId, lock: &str) {
        if !self.has_node(node) || self.dead.contains(&node) {
            return;
        }
        self.nodes[node as usize].request(self.clock, lock);
        self.collect(node);
    }

    /// Have `node` release `lock`.
    pub fn release(&mut self, node: NodeId, lock: &str) {
        if !self.has_node(node) || self.dead.contains(&node) {
            return;
        }
        self.nodes[node as usize].release(self.clock, lock);
        self.collect(node);
    }

    /// Advance logical time by `dt`, in sub-lease increments so that live nodes
    /// get to renew (and have those renewals delivered) before any lease lapses.
    /// Only a node that has stopped ticking — i.e. one that has crashed — loses
    /// its votes. After each increment all pending messages are delivered.
    pub fn advance(&mut self, dt: Instant) {
        let target = self.clock + dt;
        while self.clock < target {
            self.clock = (self.clock + RENEW_INTERVAL).min(target);
            for id in 0..self.nodes.len() as NodeId {
                if !self.dead.contains(&id) {
                    self.nodes[id as usize].tick(self.clock);
                    self.collect(id);
                }
            }
            while self.step() {}
        }
    }

    /// Deliver one in-flight message. A random non-empty link is chosen, and
    /// that link's *head* is delivered (preserving FIFO order on the link).
    /// Returns `false` when the network is idle.
    pub fn step(&mut self) -> bool {
        let mut active: Vec<(NodeId, NodeId)> = self
            .links
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(&k, _)| k)
            .collect();
        if active.is_empty() {
            return false;
        }
        // Sort for determinism (HashMap iteration order is not stable), then
        // pick via the seeded PRNG.
        active.sort_unstable();
        let key = active[(self.next_rand() as usize) % active.len()];
        let (from, to) = key;
        let msg = self.links.get_mut(&key).unwrap().pop_front().unwrap();
        self.nodes[to as usize].handle(self.clock, from, msg);
        self.collect(to);
        true
    }

    /// True if no messages are in flight.
    pub fn idle(&self) -> bool {
        self.links.values().all(|q| q.is_empty())
    }

    /// Drain locks newly acquired across all nodes since the last call, as
    /// `(node, lock, fence)`.
    pub fn drain_acquired(&mut self) -> Vec<(NodeId, LockId, Fence)> {
        let mut out = Vec::new();
        for n in &mut self.nodes {
            let id = n.id;
            for (lock, fence) in n.take_acquired() {
                out.push((id, lock, fence));
            }
        }
        out
    }

    fn collect(&mut self, from: NodeId) {
        for Outgoing { to, msg } in self.nodes[from as usize].drain_outbox() {
            // Drop anything to/from a crashed node (fail-stop + partition).
            if self.dead.contains(&from) || self.dead.contains(&to) {
                continue;
            }
            self.links.entry((from, to)).or_default().push_back(msg);
        }
    }

    fn has_node(&self, node: NodeId) -> bool {
        (node as usize) < self.nodes.len()
    }

    fn next_rand(&mut self) -> u64 {
        // xorshift64: state is always non-zero by construction.
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }
}

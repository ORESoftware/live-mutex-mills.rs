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

use std::collections::{HashMap, VecDeque};

use crate::{Fence, LockId, Message, Node, NodeId, Outgoing};

/// An in-memory cluster of [`Node`]s connected by FIFO (TCP-like) links.
///
/// Node ids are `0..n` and equal their index in [`Sim::nodes`].
pub struct Sim {
    pub nodes: Vec<Node>,
    /// One FIFO queue per directed `(from, to)` link.
    links: HashMap<(NodeId, NodeId), VecDeque<Message>>,
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
        let members: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = (0..n as NodeId)
            .map(|id| Node::new(id, members.clone()))
            .collect();
        Sim {
            nodes,
            links: HashMap::new(),
            rng: if seed == 0 { 0x1234_5678_9ABC_DEF1 } else { seed },
        }
    }

    /// Have `node` start acquiring `lock`.
    pub fn request(&mut self, node: NodeId, lock: &str) {
        self.nodes[node as usize].request(lock);
        self.collect(node);
    }

    /// Have `node` release `lock`.
    pub fn release(&mut self, node: NodeId, lock: &str) {
        self.nodes[node as usize].release(lock);
        self.collect(node);
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
        self.nodes[to as usize].handle(from, msg);
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
            self.links.entry((from, to)).or_default().push_back(msg);
        }
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

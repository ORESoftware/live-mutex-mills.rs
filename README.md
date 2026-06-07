# live-mutex-mills

A **leaderless, quorum/vote distributed mutual-exclusion** protocol — an
alternative to Raft/Paxos specialized for *locking* rather than general
state-machine replication.

Where Raft/Paxos funnel every operation through one leader's log, this protocol
has no leader: any node serves any request, independent locks run fully in
parallel, and there is no election to stall on when a node dies. The trade is
that it does *locking*, not arbitrary replicated state.

## How it works — three mechanisms, three jobs

| Mechanism | Job | Notes |
|---|---|---|
| **Quorum voting** (majority of nodes, one vote per lock held at a time) | **Safety** (mutual exclusion) | Two holders ⇒ two quorums ⇒ they intersect ⇒ a node voted twice — impossible. Uses **no timing assumptions**; safe in a fully asynchronous network, like Raft/Paxos. |
| **Lamport timestamps + INQUIRE/YIELD preemption** | **Deadlock- & starvation-freedom** | The globally-smallest request can never be preempted, so it always makes progress. Timestamps are **logical** — no physical clock anywhere on the happy path. |
| **Monotonic per-lock fence tokens** | **Downstream backstop** | Each holder's token strictly exceeds the previous (intersection again). A stale/partitioned holder is fenced out at the resource, so it can't corrupt state. |

### Acquire, in one paragraph

A node stamps a request `(lamport, node_id)` and broadcasts it to all members.
Each arbiter grants its single vote to the smallest request it knows of; when a
smaller request shows up it asks the current votee to **yield** (a votee that has
not yet locked yields; one that has locked keeps its votes until it releases).
Once a requester holds votes from a **majority**, it is in the critical section
and takes a fence token `max(seen) + 1`. On release it frees the votes, and the
arbiter — having recorded the token first — hands the lock to the next request.

## Transport requirement: FIFO channels (use TCP)

The protocol assumes **FIFO point-to-point channels** — messages between an
ordered pair of nodes arrive in send order. A single **TCP** connection per
node-pair provides exactly this. It is not a nicety; it is required: an arbiter
may send `Grant→A` then `Inquire→A`, and if those reordered, `A` could re-count
a vote that has since moved to another requester and two nodes could both reach
a quorum. The in-memory simulator (`src/sim.rs`) models this honestly: it keeps
one FIFO queue per directed link and interleaves links/locks randomly, but never
reorders within a link.

## What's here

- `src/lib.rs` — the transport-agnostic [`Node`] state machine. Pure logic, no
  I/O. It never reads a clock; callers pass `now` in, and time is used *only*
  for lease expiry on the failure path. Feed it messages with `handle`, advance
  time with `tick`, drain sends with `drain_outbox`, learn of acquisitions with
  `take_acquired` and of lost locks with `take_lost`.
- `src/sim.rs` — a deterministic, seedable FIFO network simulator with
  `crash()` and `advance()` for exercising holder failure.
- `src/codec.rs` — a dependency-free line codec for the wire.
- `src/transport.rs` + `src/bin/lmxd.rs` — a real **TCP transport** and node
  daemon. One TCP connection per directed link (= FIFO), single-threaded driver
  owning the `Node`, wall-clock-driven leases.
- `tests/` — property tests over randomized FIFO interleavings (mutual
  exclusion, progress, monotonic fencing), holder-failover recovery, codec
  round-trips, and a 3-node end-to-end test over real loopback TCP.
- `examples/contention.rs` — `cargo run --example contention`.

```
cargo test                       # all properties + failover + TCP smoke
cargo run --example contention   # watch 9 nodes hand a lock around
```

### Running a real cluster

Start one process per node; `my_id` indexes the address list:

```
lmxd 0 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
lmxd 1 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
lmxd 2 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
```

Then type `acquire <lock>` / `release <lock>` / `quit` on any node's stdin; it
prints `ACQUIRED <lock> fence=<n>` when the lock is held. Crash a holder
(`Ctrl-C`) and a survivor takes over once the lease lapses, with a higher fence.

## Known limitation: token durability window

A fence token only becomes *durable* once its `Confirm` reaches a quorum. A
holder that **crashes in the narrow window between locking and that `Confirm`
landing** can have its token reused by the next holder. The fix is to wait for a
quorum of `Confirm` acks before reporting the lock acquired (one extra round
trip); v1 reports immediately and accepts the window. Outside that window,
failover fencing is strictly monotonic (see `tests/failover.rs`).

## Roadmap

- **Confirm-ack before use** — close the token-durability window above.
- **Reconnect/buffering hardening** — the transport redials on startup but a
  mid-run connection drop currently relies on leases; add active reconnect.
- **Dynamic membership.** Quorum is `floor(n/2)+1` of a *fixed* member set.
  Changing membership safely needs one-at-a-time reconfiguration (so old and new
  quorums always overlap).
- **√n quorums (true Maekawa).** v1 broadcasts to all and needs a majority
  (`O(n)` messages). Grid/√n quorums cut that to `O(√n)` at the cost of a more
  delicate intersection structure.
- **Model checking.** The preemption/eviction logic is the subtle part; a TLA+
  or `loom`-style exhaustive check of the safety invariant would be worthwhile.

## Done

- ✅ Quorum/vote core with timestamp preemption (deadlock-free) and fencing.
- ✅ Vote leases + holder-failure recovery (physical time, failure path only).
- ✅ TCP transport + `lmxd` daemon, verified across processes.

## Design provenance

This is essentially **Maekawa's quorum mutual exclusion** (quorum intersection +
INQUIRE/YIELD with timestamp priority) plus **fence tokens** (à la Kleppmann's
"How to do distributed locking") as the resource-side backstop. It deliberately
chooses the quorum/vote design over a clock/commit-wait ("TrueTime"-style)
design because safety then rests on a logical invariant rather than a bet on
clock-skew bounds — see the design discussion that produced it.
```
                 ┌─────────── leaderless: any node serves any lock ───────────┐
   client ──LB──▶ Nk ──Request──▶ all arbiters
                          ◀──Grant (carries fence)──   (majority ⇒ enter CS)
                          ◀──Inquire / Yield──         (timestamp preemption)
                  Nk ──Confirm/Release──▶ arbiters     (token recorded, vote freed)
                 └────────────────────────────────────────────────────────────┘
```

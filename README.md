<!-- BEGIN k8s-cluster-submodule-notice -->
> [!NOTE]
> **Canonical source.** This repository is the source of truth for its code. It
> is also vendored as a **secondary** git submodule of
> [ORESoftware/k8s-cluster](https://github.com/ORESoftware/k8s-cluster) at
> `remote/submodules/live-mutex-mills.rs` — make changes here, not in that submodule checkout.
>
> On disk: source clone `~/codes/ores/live-mutex-mills.rs` · submodule checkout `~/codes/ores/k8s-cluster/remote/submodules/live-mutex-mills.rs`.
<!-- END k8s-cluster-submodule-notice --># live-mutex-mills

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
- `src/codec.rs` — selectable wire codecs: the original text line format,
  compact JSON lines, and hex-framed MessagePack.
- `src/transport.rs` + `src/bin/lmxd.rs` — a real **TCP transport** and node
  daemon. One TCP connection per directed link (= FIFO), single-threaded driver
  owning the `Node`, wall-clock-driven leases.
- `src/client_api.rs` — language-neutral client surfaces on top of `lmxd`:
  HTTP/JSON and line-oriented TCP for exclusive locks, bounded semaphores, and
  bounded-reader RW locks.
- `tests/` — property tests over randomized FIFO interleavings (mutual
  exclusion, progress, monotonic fencing), holder-failover recovery, codec
  round-trips, external HTTP/TCP client APIs, and 3-node end-to-end tests over
  real loopback TCP.
- `examples/contention.rs` — `cargo run --example contention`.

```
cargo test                       # all properties + failover + TCP smoke
cargo run --example contention   # watch 9 nodes hand a lock around
```

`lmxd` statically compiles the vendored `flags-2-env` C parser during
`cargo build` / `cargo install`, so a C compiler toolchain is required at build
time. No `flags2env` shared library is required at runtime.

### Running a real cluster

Start one process per node; `my_id` indexes the address list:

```
lmxd --client-http 127.0.0.1:9200 --client-tcp 127.0.0.1:9300 \
  0 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
lmxd --client-http 127.0.0.1:9201 --client-tcp 127.0.0.1:9301 \
  1 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
lmxd --client-http 127.0.0.1:9202 --client-tcp 127.0.0.1:9302 \
  2 127.0.0.1:9100 127.0.0.1:9101 127.0.0.1:9102
```

You can still type `acquire <lock>` / `release <lock>` / `quit` on any node's
stdin; it prints `ACQUIRED <lock> fence=<n>` when the lock is held. Pass
`--codec text`, `--codec json`, or `--codec msgpack` before `my_id` to choose
the peer-mesh wire format; `text` is the default.

`lmxd` broker flags are declared in `.cli-flags.toml` and parsed with
`ORESoftware/flags-2-env`. Named CLI flags override positional values, which
override environment variables; typed defaults are applied last. This
configuration is for the broker process, not for external clients. Run
`lmxd --help` for the generated flag table. Supported broker env keys include
`LMX_CLI_FLAGS_CONFIG`, `LMX_CODEC`, `LMX_NODE_ID`, `LMX_PEER_ADDRS`,
`LMX_CLIENT_HTTP`, `LMX_CLIENT_TCP`, `LMX_STDIN`, `LMX_LOG_INFO`,
`LMX_CONNECT_RETRY_MS`, `LMX_TICK_MS`, `LMX_MAX_FRAME_BYTES`, `LMX_DEMO`,
`LMX_DEMO_KEYS`, `LMX_DEMO_HOLD_MS`, and `LMX_DEMO_REST_MS`.

### External clients

The client API is separate from the peer-to-peer TCP mesh. Any language that can
make HTTP requests or open a TCP socket can request a lock from any `lmxd` node.
Release the returned `holder` token back to the same node endpoint that granted
it; that node is the protocol requester holding the quorum votes.

HTTP/JSON:

```
POST /locks/report-A/acquire
=> {"ok":true,"lock":"report-A","fence":1,"holder":"h0000000000000001"}

POST /locks/report-A/release
{"holder":"h0000000000000001"}
=> {"ok":true,"lock":"report-A"}
```

Line-oriented TCP:

```
ACQUIRE report-A
ACQUIRED report-A fence=1 holder=h0000000000000001
RELEASE h0000000000000001
RELEASED report-A
```

Bounded semaphores are built from exclusive permit slots. A limit-10 semaphore
allows at most ten concurrent holders for the same `(name, limit)`:

```
POST /semaphores/api-rate/acquire
{"limit":10}
=> {"ok":true,"semaphore":"api-rate","limit":10,"permit":3,"fence":7,"holder":"h0000000000000009"}

SEMACQUIRE 10 api-rate
SEMACQUIRED api-rate limit=10 permit=4 fence=8 holder=h000000000000000a
```

Semaphore limits are intentionally capped at 2..100; use exclusive locks for
limit 1. Callers must use the same `(name, limit)` to contend for the same
semaphore namespace.

RW locks use the same bounded-slot idea for readers. A read acquire takes one
reader permit; a write acquire takes an admission gate and then all reader
permits. That means readers can share up to `limit`, writers are exclusive, and
once a writer reaches the gate, later readers wait behind it:

```
POST /rwlocks/report-A/read-acquire
{"limit":10}
=> {"ok":true,"rwlock":"report-A","mode":"read","limit":10,"permit":3,"fence":12,"holder":"rw0000000000000001"}

POST /rwlocks/report-A/write-acquire
{"limit":10}
=> {"ok":true,"rwlock":"report-A","mode":"write","limit":10,"fence":18,"holder":"rw0000000000000002"}

RACQUIRE 10 report-A
RWACQUIRED report-A mode=read limit=10 permit=4 fence=13 holder=rw0000000000000003

WACQUIRE 10 report-A
RWACQUIRED report-A mode=write limit=10 fence=19 holder=rw0000000000000004
```

RW lock limits are intentionally capped at 2..100. Readers and writers must use
the same `(name, limit)` to contend for the same RW lock namespace.

Crash a holder (`Ctrl-C`) and a survivor takes over once the lease lapses, with
a higher fence.

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
- **Model checking.** The preemption/eviction logic is the subtle part; a TLA+
  or `loom`-style exhaustive check of the safety invariant would be worthwhile.

## Done

- ✅ Quorum/vote core with timestamp preemption (deadlock-free) and fencing.
- ✅ Vote leases + holder-failure recovery (physical time, failure path only).
- ✅ TCP transport + `lmxd` daemon, verified across processes.
- ✅ External HTTP/TCP client APIs for exclusive locks, bounded semaphores, and
  bounded-reader RW locks.
- ✅ **√n grid quorums (true Maekawa).** Opt-in via `--quorum-policy grid` /
  `LMX_QUORUM_POLICY=grid` (default stays majority). Each node's quorum is its
  row∪column in a `ceil(√n)×ceil(√n)` grid (≈2√n−1 nodes, `O(√n)` messages);
  any two quorums intersect, so mutual exclusion is preserved. Proven in
  `tests/quorum_grid.rs`; see `docs/sqrt-n-quorum-design.md` (incl. the
  availability trade-off vs majority).

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

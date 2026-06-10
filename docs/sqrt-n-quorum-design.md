# Design: moving off majority quorum → √n grid Maekawa quorums

Status: **IMPLEMENTED in BOTH maekawa engines** (behind `LMX_QUORUM_POLICY=grid`,
default `majority`); proven by tests in each. `live-mutex-mills.rs/src/lib.rs`
(Rust) and `live-mutex.distributed/src/distributed/node.ts` (TS, line-for-line
port) carry the identical change.

### TS port (`live-mutex.distributed`, 2026-06-09)

Same shape as the Rust change: `QuorumPolicy = 'majority' | 'grid'` + `gridQuorum()`
in `src/distributed/node.ts`; requester uses `targets`/`threshold`; threaded
through `LMXDistributedNode` (transport) and `lmxd` (`LMX_QUORUM_POLICY` env).
Tests: `test/distributed/quorum-grid-test.ts` (intersection for many n incl.
non-squares; ME/progress/fence property test under grid for n∈{4,5,9,10,16,17}).
Default stays majority — the existing `mutual-exclusion-test.ts` still passes.
Deploy knob: `deploy/k8s/statefulset.yaml` `LMX_QUORUM_POLICY`.

### What landed in mills.rs (2026-06-09)

- `QuorumPolicy { Majority, Grid }` + `grid_quorum()` in `src/lib.rs`; the
  requester now sends to / waits for a configurable `targets`/`threshold`
  (Majority = all members + ⌊n/2⌋+1; Grid = row∪column + all of it). Arbiter,
  leases, fencing, INQUIRE/YIELD unchanged. Default is **Majority** — fully
  backward compatible (all pre-existing tests pass).
- Selectable via `--quorum-policy grid` / `LMX_QUORUM_POLICY=grid`
  (`src/broker_runtime_config.rs`, threaded through `TransportSettings`).
- Tests (`tests/quorum_grid.rs`): exhaustive pairwise-intersection for
  n ∈ {1..11,15,16,17,24,25,26,31,36}; quorum size = 2√n−1 for squares; and the
  full mutual-exclusion/progress/fence-monotonicity property test under Grid for
  n ∈ {4,5,9,10,16,17} × 40 seeds. Plus a real-TCP 3-node smoke under Grid.
- k8s: `deploy/k8s/statefulset.yaml` exposes `LMX_QUORUM_POLICY` (set `grid` +
  replicas=9 for a 3×3 gate).

Remaining: port the identical change to `node.ts`; optionally run the k8s n=9
grid gate.

## 1. Current state (what we have)

Despite the "Maekawa" framing, v1 is **quorum-by-count**, not quorum-by-set:

- every node knows the full `members` list; `quorum = floor(n/2) + 1`
  (`lib.rs:212`, `node.ts:119`);
- `request()` **broadcasts** the vote request to **all** members
  (`lib.rs:235-253`, `node.ts:127-143`) — O(n) messages;
- a requester enters the critical section when `votes.len() >= quorum`
  (`lib.rs:587`, `node.ts` acquire path) — i.e. any majority-sized subset;
- safety rests on the intersection of any two **majorities** (q + q > n).

So today's "quorum" is *a strict majority by count*, with all-to-all messaging.

## 2. ⚠️ What √n grid quorums DO and DO NOT buy (read this first)

This is the crux, because it determines whether √n is even the right lever for
the stated goal ("move away from majority; we shouldn't strictly require a
majority to operate").

**√n grid quorums change the quorum STRUCTURE, not the requirement to have a
quorum.** Arrange the N nodes in a √N×√N grid; node *i*'s quorum `Q_i` is its
row ∪ its column (size ≈ 2√N − 1). Any two quorums intersect (one's row crosses
the other's column), so safety is preserved with **smaller** quorums and
**O(√n)** messages instead of O(n).

What it buys: **message complexity** (O(√n) per acquire) and smaller quorums.

What it does **NOT** buy — and this is the important part:

- It does **not** let the system "operate without a majority alive." Every
  quorum system requires any two quorums to intersect; that is mathematically
  incompatible with two disjoint groups both making progress. You cannot have a
  *safe* quorum mutex that keeps granting when the live nodes can't form an
  intersecting quorum.
- Grid quorums are in fact **LESS available than majority for some failures**:
  if an entire **row or column** is down, some nodes can form **no** quorum at
  all, even though a *majority* of nodes may still be alive. Majority quorums
  tolerate *any* `floor((n-1)/2)` failures; grid quorums tolerate some larger
  failures but are wedged by specific small ones (a full line).

**So if the real goal is "keep operating when fewer than a majority are alive,"
√n grid is the wrong tool.** The only safe ways to operate below an intersecting
quorum are: (a) deliberately allow split-brain and rely on the **fence-token
backstop** (already present — a stale holder is fenced out at the resource), or
(b) let the **central raft brain reconfigure/assign quorums** dynamically (the
end-state in the todos). See §6.

If the goal is specifically **fewer messages / smaller quorums while keeping
safety**, √n grid is exactly right. Recommend confirming intent before coding.

## 3. Target design (√n grid)

Introduce a **quorum assignment** abstraction — the seam that both the grid and a
future raft brain plug into:

```
trait QuorumPolicy {
    /// The quorum set this node must collect votes from to hold a lock.
    fn quorum_of(&self, node: NodeId) -> &HashSet<NodeId>;
}
```

- `MajorityPolicy` — today's behavior (kept for compat / fallback): `quorum_of`
  is conceptually "any majority"; we special-case it as count-based.
- `GridPolicy` — deterministic √n grid derived from the (ordered) member list, so
  **every node computes the identical grid** with no coordination:
  - `side = ceil(sqrt(n))`; place members row-major into a `side × side` grid;
  - `Q_i = { members in row(i) } ∪ { members in col(i) }`, skipping empty cells;
  - guarantee: `Q_i ∩ Q_j ≠ ∅` for all i,j (proof in §4).

### Code change points (identical shape in both engines)

1. **Construction** (`lib.rs:202-217`, `node.ts:115-120`): build the grid quorum
   sets from `members`; store `my_quorum = quorum_of(self.id)`.
2. **request()** (`lib.rs:252`, `node.ts:142`): send `Request` only to
   `my_quorum`, not all members.
3. **Acquire condition** (`lib.rs:587`, `node.ts` acquire): enter CS when
   `r.votes ⊇ my_quorum` (have a grant from **every** node in my quorum set),
   replacing `votes.len() >= quorum`.
4. **Loss detection** (`lib.rs:665`, `node.ts:228/455`): a held lock is lost when
   `r.votes` no longer ⊇ `my_quorum` (any quorum member revoked), replacing
   `votes.len() < quorum`.
5. **tick() rebroadcast** (`lib.rs:350-362`, `node.ts:228-238`): rebroadcast to
   `my_quorum`, and renew held votes to `my_quorum`.
6. The **arbiter side is unchanged**: each node still grants its single vote to
   the smallest-timestamp requester it knows of, with INQUIRE/YIELD preemption.
   That machinery already provides deadlock-freedom and is independent of how
   requesters choose their quorum.

The change is localized to the **requester** role + construction; the arbiter,
fence tokens, leases, and INQUIRE/YIELD all carry over unchanged.

## 4. Safety: intersection proof

Place members row-major in a `side × side` grid (`side = ceil(√n)`). For node
*i* in cell `(r_i, c_i)`, `Q_i = row(r_i) ∪ col(c_i)`.

Take any two nodes i, j. Consider cell `(r_i, c_j)` — the intersection of i's row
and j's column. If that cell is occupied, its member is in `row(r_i) ⊆ Q_i` and
in `col(c_j) ⊆ Q_j`, so `Q_i ∩ Q_j ≠ ∅`. ∎ (for the partially-filled last row,
use the symmetric cell `(r_j, c_i)`; the construction must guarantee at least one
of the two crossing cells is occupied — enforced by how we pack the grid, and
asserted by a property test, see §7.)

Two simultaneous holders would each hold all votes of their quorum; the shared
node in `Q_i ∩ Q_j` would have granted its single vote twice — impossible. So
mutual exclusion is preserved exactly as with majorities.

## 5. Liveness / deadlock

Maekawa quorum-set mutex can deadlock (cyclic wait among partially-granted
quorums) **without** preemption; v1 already implements Lamport-timestamp priority
with INQUIRE/YIELD, so the globally-smallest request can never be preempted and
always completes. That argument is independent of quorum shape, so it carries
over. We must, however, re-verify it under the property tests (§7), because the
*set* of nodes a request contends with changes from "all" to "my row+col."

Availability caveat from §2 (a downed row/column wedging some nodes) is a
liveness/availability property, not a safety one — call it out in the README.

## 6. Forward hook: the central raft brain

The `QuorumPolicy` seam is exactly where the future "central raft brain" plugs
in. Instead of deriving quorums from a static grid, a small raft cluster can:

- own the authoritative **membership + quorum assignment** (or per-node vote
  **weights**), and hand each node its `quorum_of` set;
- safely **reconfigure** quorums one step at a time (old and new quorum systems
  must overlap during transitions — the same constraint as dynamic membership);
- optionally let non-raft nodes run as **learners** that follow the brain's
  assignment.

This is the path to "operate flexibly without a fixed majority" *safely*: the
brain re-forms intersecting quorums around the live set, rather than pretending
intersection isn't needed. Designing `QuorumPolicy` now makes that a drop-in
later.

## 7. Test plan

- **Property test (safety):** extend `tests/mutual_exclusion.rs` /
  `test/distributed/mutual-exclusion-test.ts` to run with `GridPolicy` for
  n ∈ {4, 5, 9, 10, 16, 17} (squares and non-squares); assert never-two-holders
  and fence monotonicity under randomized FIFO delivery.
- **Intersection unit test:** for those n, assert `Q_i ∩ Q_j ≠ ∅` for *all*
  pairs — the structural guarantee the safety proof depends on.
- **Liveness test:** the globally-smallest request always eventually acquires
  (no deadlock) under contention.
- **Availability characterization (new, expected-fail-by-design):** document that
  a full-row/full-column outage wedges specific nodes — encode it as a test that
  asserts the *expected* unavailability so the tradeoff is explicit, not a
  surprise.
- **k8s gate:** the existing `deploy/` verify Jobs already assert fence
  uniqueness under contention; rerun them with a GridPolicy build for n=9 (a
  3×3 grid) once implemented.

## 8. Recommendation

1. Land the `QuorumPolicy` seam + `GridPolicy` behind a config/flag, defaulting
   to majority, so it's opt-in and reversible.
2. Prove it with the property + intersection tests before any cluster rollout.
3. **Confirm the intent** (message-complexity vs availability — §2) before
   investing, because if the goal is availability-below-majority, the grid is not
   the answer and we should jump straight to the raft-brain `QuorumPolicy`.

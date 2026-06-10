# live-mutex-mills — cluster audit findings

Findings from standing the cluster up for real on k3d (3 nodes, cross-node) and
driving the external HTTP client API under contention. These are about the
**external client surface**, not the core quorum algorithm — the in-process
property tests (`tests/mutual_exclusion.rs`, `tests/failover.rs`) already cover
the algorithm, and the live cluster confirms it holds over a real network.

## Finding 1 — the HTTP `holder` token is node-local (acquire/release affinity required)

`POST /locks/{lock}/acquire` returns an opaque `holder` token. That token is
recorded in the **coordinator state of the node that served the request**. Sending
the matching `POST /locks/{lock}/release` to a *different* node returns:

```
{"ok":false,"error":"not_held","message":"holder token does not refer to a held lock"}
```

The lock **state** is correctly distributed across the quorum (any node can serve
any lock), but the **client handle** is not. A client must send `acquire` and
`release` to the same node.

**Deployment implication.** A plain round-robin `Service` in front of the API is
incorrect — release will frequently hit the wrong pod and fail. Mitigations:
- `sessionAffinity: ClientIP` on the client Service (done in
  `deploy/k8s/service-client.yaml`) so a source IP sticks to one backend, or
- clients address a specific pod via the headless Service
  (`lmx-<ordinal>.lmx-peers:6971`).

**Possible code fix (out of scope for #2).** Make the holder token globally
resolvable (encode the owning node + forward release to it), so the API is truly
node-agnostic and matches the "any node serves any lock" claim end-to-end.

## Finding 2 — an external client that dies without releasing holds the lock forever

The lease mechanism (`LEASE = 10s`, renewed every `RENEW_INTERVAL = 2s`) recovers
locks when a **node** crashes: the arbiters' vote leases lapse, the holder drops
below quorum, and the lock is reclaimed (`tests/failover.rs`).

It does **not** recover from an external **client** crash. When an HTTP client
calls `acquire` and then dies without `release`, the serving **node** still
believes it holds the lock for that holder and **keeps renewing the lease**. The
client's death is invisible to the node (the acquire HTTP request already
completed; nothing ties holder lifetime to the client connection). The lock stays
held until an explicit `release` or until the whole node is restarted.

This was observed directly: a verify run was killed mid-flight, and every
subsequent run blocked forever on the first `acquire` of the same lock name.

**Why it matters.** Client-crash-without-release is *the* canonical distributed-lock
failure mode (cf. Kleppmann, "How to do distributed locking"). Holding the lock
indefinitely defeats the purpose.

**Mitigations / possible fixes (out of scope for #2):**
- Add a **held-lock TTL** (acquire-with-lease at the API level; auto-release if
  the client doesn't heartbeat), distinct from the internal vote lease.
- Tie holder lifetime to the **client TCP connection** (release on disconnect).
- `acquire_timeout` exists but bounds the *wait to acquire*, not the *held* time.

The fence-token backstop still protects downstream resources (a stale holder is
fenced out), but liveness of the lock itself is lost until manual intervention.

## What the live cluster *does* prove

- 3 lmxd pods form the peer mesh across 3 separate k8s nodes over real TCP.
- Under heavy contention on a single hot lock entered via all 3 nodes, every
  granted fence token is **unique and strictly increasing** — i.e. the broker
  never double-grants, so mutual exclusion holds over the network. (See the
  `deploy/k8s/verify-job.yaml` gate.)

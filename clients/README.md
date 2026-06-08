# live-mutex-mills clients

`live-mutex-mills` is **leaderless**: there is no central broker to point a
socket client at. Every node is a peer in a quorum/vote mesh, and the
daemon's own client surface is its **stdin/stdout protocol** (see the repo
README, "Then type commands on stdin"):

```
stdin  : acquire <lock> | release <lock> | quit
stdout : ACQUIRED <lock> fence=<n> | LOST <lock> | # <info>
```

So the clients here are thin **cluster harnesses**: they launch a local N-node
`lmxd` cluster over loopback TCP and drive it through that stdin/stdout
interface — demonstrating the leaderless acquire → handoff → release lifecycle
and the strictly-monotonic per-lock fence tokens the protocol guarantees across
node handoffs.

| Shell        | Path           | Smoke command                              |
|--------------|----------------|--------------------------------------------|
| Shell (bash) | `shell/`       | `./clients/shell/smoke.sh`                 |
| PowerShell   | `powershell/`  | `pwsh ./clients/powershell/smoke.ps1`      |

Both are dependency-free apart from bash / PowerShell and the `lmxd` binary
built from this repo (the harness builds it with `cargo build --release --bin
lmxd` if it isn't already present under `target/`).

## Smoke test

```bash
./clients/shell/smoke.sh
```

Sample run (3-node cluster, text codec):

```
[smoke-shell] starting 3-node lmxd cluster (codec=text)
[smoke-shell] node 0 mesh:
# node 0 of 3 starting with text codec
# connected to node 1
# connected to node 2
[smoke-shell] node 0 ACQUIRED smoke-lock fence=1
[smoke-shell] node 0 released smoke-lock
[smoke-shell] node 1 ACQUIRED smoke-lock fence=2
[smoke-shell] fence strictly increased across handoff (1 -> 2)
[smoke-shell] OK
```

Tunables (environment): `LMX_NODES` (cluster size; quorum is `floor(n/2)+1`),
`LMX_BASE_PORT` (node *i* listens on `BASE+i`), `LMX_CODEC` (`text` | `json` |
`msgpack`).

## Using the harness from your own script

```bash
. clients/shell/lmxd_cluster.sh

lmx_start_cluster 3
lmx_acquire 0 my-lock          # blocks until "ACQUIRED my-lock fence=N"
echo "held with fence $LMX_FENCE"
lmx_release 0 my-lock
lmx_stop_cluster               # also wired to an EXIT trap in smoke.sh
```

# live-mutex-mills.rs — clients audit (2026-06-09)

This repo's `clients/` are **cluster harnesses**, not per-language protocol
clients: `shell/` and `powershell/` build the `lmxd` binary, start a local
N-node cluster over loopback TCP, and drive an acquire → handoff → release
lifecycle, asserting strictly-increasing fence tokens.

## Result

| Harness | Result | Notes |
|---|---|---|
| Shell (`clients/shell/smoke.sh`) | **PASS** | 3-node cluster; lock handed off node-0 → node-1 with fence 1 → 2 (strictly increased) |
| PowerShell (`clients/powershell/smoke.ps1`) | blocked | no `pwsh` installed (logic mirrors the shell harness) |

The shell smoke is self-contained (no external broker) and exercises the real
multi-process mesh — a good complement to the in-process property tests
(`tests/mutual_exclusion.rs`, `tests/failover.rs`) and the k8s cluster gate
(`deploy/`).

There are no other language clients in this repo. (A future √n / HTTP client
surface would let the richer multi-language client suites from the sibling repos
target this broker too.)

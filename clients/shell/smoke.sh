#!/usr/bin/env bash
# End-to-end smoke test for the live-mutex-mills shell client.
#
#   ./smoke.sh
#
# Launches a local N-node lmxd cluster (default 3) over loopback, then exercises
# the leaderless lock lifecycle through the daemon's stdin/stdout interface:
# acquire a lock on one node (observe its fence token), release it, then
# re-acquire from a *different* node and assert the fence strictly increased
# (the monotonic per-lock fencing the protocol guarantees across handoffs).
#
# Tunables: LMX_NODES, LMX_BASE_PORT, LMX_CODEC (text|json|msgpack).

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lmxd_cluster.sh
. "$HERE/lmxd_cluster.sh"

trap lmx_stop_cluster EXIT

N="${LMX_NODES:-3}"
echo "[smoke-shell] starting ${N}-node lmxd cluster (codec=${LMX_CODEC})"
lmx_start_cluster "$N"
echo "[smoke-shell] node 0 mesh:"; { lmx_node_log 0 | grep -E '^#' || true; } | head -n "$N" || true

lmx_acquire 0 smoke-lock
first="$LMX_FENCE"
echo "[smoke-shell] node 0 ACQUIRED smoke-lock fence=${first}"
lmx_release 0 smoke-lock
echo "[smoke-shell] node 0 released smoke-lock"

# Hand the same lock to a different node; its fence must strictly exceed the first.
target=$(( N > 1 ? 1 : 0 ))
lmx_acquire "$target" smoke-lock
second="$LMX_FENCE"
echo "[smoke-shell] node ${target} ACQUIRED smoke-lock fence=${second}"
lmx_release "$target" smoke-lock

if ! [ "$second" -gt "$first" ] 2>/dev/null; then
  echo "[smoke-shell] FAIL: fence must strictly increase across handoff (${first} -> ${second})" >&2
  exit 1
fi
echo "[smoke-shell] fence strictly increased across handoff (${first} -> ${second})"

echo "[smoke-shell] OK"

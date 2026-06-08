#!/usr/bin/env bash
# lmxd_cluster.sh — Bash harness/client for the live-mutex-mills `lmxd` daemon.
#
# Unlike a classic broker, live-mutex-mills is *leaderless*: there is no central
# server to point a socket client at. Every node is a peer in a quorum/vote mesh,
# and the documented client surface is the daemon's own stdin/stdout protocol
# (README "Then type commands on stdin"):
#
#     stdin  : acquire <lock> | release <lock> | quit
#     stdout : ACQUIRED <lock> fence=<n> | LOST <lock> | # <info>
#
# So this client launches a local N-node cluster over loopback TCP (one FIFO per
# node for stdin) and drives it through exactly that interface. The only
# dependency is bash plus the `lmxd` binary (built from this repo with cargo).
# Source this file and call the lmx_* functions; see smoke.sh for usage.

# LMX_FENCE is part of the public API (set by lmx_acquire).
# shellcheck disable=SC2034

: "${LMX_NODES:=3}"          # cluster size (quorum is floor(n/2)+1)
: "${LMX_BASE_PORT:=9300}"   # first loopback port; node i listens on BASE+i
: "${LMX_CODEC:=text}"       # text | json | msgpack
: "${LMX_WAIT:=20}"          # seconds to wait for an ACQUIRED line

LMX_RUNDIR=""    # per-run scratch dir (FIFOs, logs, pids)
LMX_BIN=""       # resolved lmxd binary
LMX_FENCE=""     # fence token from the last lmx_acquire

# Locate (building if necessary) the lmxd binary in this repo.
lmx_resolve_bin() {
  local repo; repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  if [ -x "$repo/target/release/lmxd" ]; then LMX_BIN="$repo/target/release/lmxd"; return 0; fi
  if [ -x "$repo/target/debug/lmxd" ]; then LMX_BIN="$repo/target/debug/lmxd"; return 0; fi
  echo "[lmx] building lmxd (cargo build --release --bin lmxd) ..." >&2
  ( cd "$repo" && cargo build --release --bin lmxd >&2 ) || return 1
  LMX_BIN="$repo/target/release/lmxd"
}

# lmx_start_cluster [n]  — launch n nodes. Each node's stdin is a FIFO held open
# on fd (10+i) so the daemon never sees EOF; commands are written to the FIFO
# path (no eval on user data).
lmx_start_cluster() {
  local n="${1:-$LMX_NODES}"
  lmx_resolve_bin || { echo "[lmx] could not build/find lmxd" >&2; return 1; }
  LMX_RUNDIR="$(mktemp -d "${TMPDIR:-/tmp}/lmxd-cluster.XXXXXX")"
  local addrs=() i
  for ((i = 0; i < n; i++)); do addrs+=("127.0.0.1:$((LMX_BASE_PORT + i))"); done
  for ((i = 0; i < n; i++)); do
    mkfifo "$LMX_RUNDIR/n$i.in"
    "$LMX_BIN" --codec "$LMX_CODEC" "$i" "${addrs[@]}" \
      <"$LMX_RUNDIR/n$i.in" >"$LMX_RUNDIR/n$i.out" 2>&1 &
    echo $! >"$LMX_RUNDIR/n$i.pid"
    disown 2>/dev/null || true   # silence bash "Terminated" notices on teardown
    # Hold each node's stdin FIFO open on a fixed fd so the reader never hits
    # EOF. eval is required for a dynamic fd on bash 3.2 (no {var} redirections);
    # both operands are trusted here (numeric fd + a mktemp path).
    eval "exec $((10 + i))>\"\$LMX_RUNDIR/n\$i.in\""
  done
  sleep 2   # let the mesh dial up (logs print "connected to node N")
}

# lmx_node_send <id> <command...>  — append one command line to a node's stdin.
lmx_node_send() {
  local id="$1"; shift
  printf '%s\n' "$*" >"$LMX_RUNDIR/n$id.in"
}

# Wait until <file> contains the literal <pattern>, up to LMX_WAIT seconds.
_lmx_wait_for() {
  local f="$1" pat="$2" t="${3:-$LMX_WAIT}" i=0
  while [ "$i" -lt $((t * 10)) ]; do
    grep -qF -- "$pat" "$f" 2>/dev/null && return 0
    sleep 0.1; i=$((i + 1))
  done
  return 1
}

# lmx_acquire <node_id> <lock>  — drives acquire on a node; sets LMX_FENCE.
lmx_acquire() {
  local id="$1" lock="$2"
  lmx_node_send "$id" "acquire $lock"
  _lmx_wait_for "$LMX_RUNDIR/n$id.out" "ACQUIRED $lock fence=" || {
    echo "[lmx] timed out waiting for ACQUIRED $lock on node $id" >&2; return 1; }
  # Exact-match the lock name (field compare, so regex metacharacters in the
  # lock name are harmless) and read the fence off the latest grant line.
  LMX_FENCE="$(awk -v L="$lock" '
    $1 == "ACQUIRED" && $2 == L {
      for (i = 3; i <= NF; i++) if ($i ~ /^fence=/) { t = $i; sub(/^fence=/, "", t); f = t }
    }
    END { if (f != "") print f }' "$LMX_RUNDIR/n$id.out")"
}

# lmx_release <node_id> <lock>
lmx_release() { lmx_node_send "$1" "release $2"; }

# lmx_node_log <node_id>  — echo a node's captured stdout (for inspection).
lmx_node_log() { cat "$LMX_RUNDIR/n$1.out" 2>/dev/null; }

lmx_stop_cluster() {
  [ -n "$LMX_RUNDIR" ] || return 0
  local f pid
  for f in "$LMX_RUNDIR"/n*.pid; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"; kill "$pid" 2>/dev/null || true
  done
  sleep 0.3
  for f in "$LMX_RUNDIR"/n*.pid; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"; kill -9 "$pid" 2>/dev/null || true
  done
  rm -rf "$LMX_RUNDIR"
  LMX_RUNDIR=""
  return 0
}

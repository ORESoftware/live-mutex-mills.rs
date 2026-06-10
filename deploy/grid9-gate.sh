#!/usr/bin/env bash
# Validate the √n GRID quorum policy on a real 9-node (3×3) cluster on k3d:
# build the grid-capable image, deploy 9 replicas with LMX_QUORUM_POLICY=grid,
# confirm every node reports "quorum Grid", then run the contention gate
# (fence-uniqueness == mutual exclusion under grid quorums over the network).
set -euo pipefail
export KUBECTL_NO_CONFIRM=1
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
CLUSTER="live-mutex-mills"; IMAGE="live-mutex-mills:dev"; NS="live-mutex-mills"

echo "==> ensure cluster"
k3d cluster list "$CLUSTER" >/dev/null 2>&1 || k3d cluster create --config deploy/k3d/cluster.yaml

echo "==> build grid-capable image + import"
docker build -f deploy/Dockerfile -t "$IMAGE" .
k3d image import "$IMAGE" -c "$CLUSTER"

echo "==> apply base, then scale to 9 (3×3) with grid policy"
kubectl apply -k deploy/k8s
kubectl -n "$NS" patch statefulset lmx -p '{
  "spec":{"replicas":9,"template":{"spec":{"containers":[{"name":"lmxd","env":[
    {"name":"LMX_REPLICAS","value":"9"},
    {"name":"LMX_QUORUM_POLICY","value":"grid"}
  ]}]}}}}'

echo "==> wait for 9-pod rollout"
kubectl -n "$NS" rollout status statefulset/lmx --timeout=240s
kubectl -n "$NS" get pods -o wide | grep lmx-

echo "==> confirm GRID policy is actually in effect (from node logs)"
# RollingUpdate restarts pods in reverse-ordinal order; the last one (lmx-0) may
# not have printed its startup line yet, so retry until all 9 report (or time out).
grid_count=0
for _ in $(seq 1 20); do
  grid_count=0
  for i in $(seq 0 8); do
    kubectl -n "$NS" logs "lmx-$i" 2>/dev/null | grep -q "quorum Grid" && grid_count=$((grid_count+1))
  done
  [ "$grid_count" = 9 ] && break
  sleep 2
done
echo "    $grid_count/9 nodes report 'quorum Grid'"
[ "$grid_count" = 9 ] || { echo "FAIL: not all nodes are running grid policy"; exit 1; }

echo "==> run contention gate against the 9-node grid cluster"
kubectl -n "$NS" delete job lmx-verify --ignore-not-found
kubectl -n "$NS" delete configmap lmx-verify --ignore-not-found
kubectl -n "$NS" apply -f deploy/k8s/verify-job.yaml
kubectl -n "$NS" wait --for=condition=complete job/lmx-verify --timeout=240s 2>/dev/null &
cpid=$!
kubectl -n "$NS" wait --for=condition=failed job/lmx-verify --timeout=240s 2>/dev/null &
fpid=$!
wait -n "$cpid" "$fpid" || true
kubectl -n "$NS" logs job/lmx-verify
kubectl -n "$NS" get job lmx-verify -o jsonpath='{.status.succeeded}' | grep -q 1 \
  && echo "==> GRID 3×3 GATE PASSED" || { echo "==> GRID 3×3 GATE FAILED"; exit 1; }

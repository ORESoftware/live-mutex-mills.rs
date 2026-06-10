#!/usr/bin/env bash
# Stand up the live-mutex-mills leaderless cluster locally on k3d and run the
# correctness gate. Idempotent: re-running rebuilds the image and re-imports it.
#
#   deploy/up.sh            # create cluster, build, deploy, verify
#   deploy/up.sh --verify   # just (re-)run the verify Job against a live cluster
set -euo pipefail

# Some environments wrap kubectl with an interactive confirmation guard.
export KUBECTL_NO_CONFIRM=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CLUSTER="live-mutex-mills"
IMAGE="live-mutex-mills:dev"
NS="live-mutex-mills"

run_verify() {
  echo "==> applying verify Job"
  kubectl -n "$NS" delete job lmx-verify --ignore-not-found
  kubectl -n "$NS" apply -f deploy/k8s/verify-job.yaml
  echo "==> waiting for verify Job to finish"
  kubectl -n "$NS" wait --for=condition=complete job/lmx-verify --timeout=240s &
  cpid=$!
  kubectl -n "$NS" wait --for=condition=failed job/lmx-verify --timeout=240s &
  fpid=$!
  wait -n "$cpid" "$fpid" || true
  kubectl -n "$NS" logs job/lmx-verify
  # propagate the Job's verdict as this script's exit code
  if kubectl -n "$NS" get job lmx-verify -o jsonpath='{.status.succeeded}' | grep -q 1; then
    echo "==> VERIFY PASSED"; return 0
  else
    echo "==> VERIFY FAILED"; return 1
  fi
}

if [[ "${1:-}" == "--verify" ]]; then
  run_verify; exit $?
fi

echo "==> ensuring k3d cluster '$CLUSTER'"
if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
  k3d cluster create --config deploy/k3d/cluster.yaml
else
  echo "    cluster exists; reusing"
fi

echo "==> building image $IMAGE (context = repo root)"
docker build -f deploy/Dockerfile -t "$IMAGE" .

echo "==> importing image into k3d"
k3d image import "$IMAGE" -c "$CLUSTER"

echo "==> applying manifests"
kubectl apply -k deploy/k8s

echo "==> waiting for StatefulSet rollout"
kubectl -n "$NS" rollout status statefulset/lmx --timeout=240s

echo "==> pods:"
kubectl -n "$NS" get pods -o wide

run_verify

#!/bin/sh
# Entrypoint for an `lmxd` node running as a StatefulSet pod.
#
# live-mutex-mills uses a FIXED, POSITIONAL membership: `lmxd <my_id> <addr_0>
# ... <addr_{n-1}>`, where my_id indexes the address list. A StatefulSet gives
# us exactly that: stable ordinal pod names (lmx-0, lmx-1, ...) and stable
# per-pod DNS via a headless Service. So we:
#   1. derive our ordinal -> LMX_NODE_ID
#   2. synthesize the full, identical peer-address list for every pod
#   3. wait for our OWN DNS A record (lmxd binds addrs[id] literally, so the
#      name must resolve to this pod's IP before bind() can succeed)
#
# Inputs (set by the StatefulSet):
#   POD_NAME, POD_NAMESPACE      (downward API)
#   LMX_PEERS_SVC                headless Service name (DNS subdomain)
#   LMX_REPLICAS                 cluster size N
#   LMX_PEER_PORT                peer-mesh port           (default 9000)
#   LMX_CLUSTER_DOMAIN           cluster DNS domain       (default cluster.local)
#   LMX_CLIENT_HTTP_PORT         client HTTP API port     (default 6971)
set -eu

ORDINAL="${POD_NAME##*-}"          # lmx-2  -> 2
STS_NAME="${POD_NAME%-*}"          # lmx-2  -> lmx
PEER_PORT="${LMX_PEER_PORT:-9000}"
DOMAIN="${LMX_CLUSTER_DOMAIN:-cluster.local}"
HTTP_PORT="${LMX_CLIENT_HTTP_PORT:-6971}"

# Build the address list (identical on every node; my_id selects self).
addrs=""
i=0
while [ "$i" -lt "$LMX_REPLICAS" ]; do
  fqdn="${STS_NAME}-${i}.${LMX_PEERS_SVC}.${POD_NAMESPACE}.svc.${DOMAIN}"
  addrs="${addrs:+$addrs,}${fqdn}:${PEER_PORT}"
  i=$((i + 1))
done

self_fqdn="${STS_NAME}-${ORDINAL}.${LMX_PEERS_SVC}.${POD_NAMESPACE}.svc.${DOMAIN}"

# lmxd binds addrs[id] = our own FQDN, so wait until kube-dns can resolve us.
echo "# waiting for own DNS record ${self_fqdn} ..."
until getent hosts "$self_fqdn" >/dev/null 2>&1; do
  sleep 0.5
done
echo "# DNS ready: $(getent hosts "$self_fqdn")"

export LMX_NODE_ID="$ORDINAL"
export LMX_PEER_ADDRS="$addrs"
export LMX_CLIENT_HTTP="0.0.0.0:${HTTP_PORT}"
export LMX_STDIN="off"           # daemon mode: no stdin command loop
export LMX_LOG_INFO="${LMX_LOG_INFO:-on}"
export LMX_CODEC="${LMX_CODEC:-json}"

echo "# node ${ORDINAL}/${LMX_REPLICAS} peers=[${addrs}] http=:${HTTP_PORT}"
exec lmxd

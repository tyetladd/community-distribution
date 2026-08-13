#!/usr/bin/env bash
# Workaround for Docker Engine 27+ defaulting FORWARD to DROP, which blocks
# cross-node pod traffic in multi-node kind clusters (the "kind" docker
# network's own bridge forwards traffic between node containers, and that
# traffic needs an explicit DOCKER-USER accept). Safe to re-run: skips if
# the network/bridge isn't up yet, no-ops if the rule already exists.
set -euo pipefail

NETWORK_NAME="${KIND_DOCKER_NETWORK:-kind}"
MAX_WAIT_SECONDS=60
SLEEP_INTERVAL=2

net_id=""
elapsed=0
while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do
  net_id="$(docker network inspect "$NETWORK_NAME" -f '{{.Id}}' 2>/dev/null || true)"
  [ -n "$net_id" ] && break
  sleep "$SLEEP_INTERVAL"
  elapsed=$((elapsed + SLEEP_INTERVAL))
done

if [ -z "$net_id" ]; then
  echo "kind-docker-user-fix: docker network '$NETWORK_NAME' not found after ${MAX_WAIT_SECONDS}s, skipping" >&2
  exit 0
fi

bridge="br-${net_id:0:12}"

if ! ip link show "$bridge" >/dev/null 2>&1; then
  echo "kind-docker-user-fix: bridge interface $bridge not found, skipping" >&2
  exit 0
fi

if iptables -C DOCKER-USER -i "$bridge" -o "$bridge" -j ACCEPT 2>/dev/null; then
  echo "kind-docker-user-fix: rule already present for $bridge"
else
  iptables -I DOCKER-USER -i "$bridge" -o "$bridge" -j ACCEPT
  echo "kind-docker-user-fix: added ACCEPT rule for $bridge"
fi

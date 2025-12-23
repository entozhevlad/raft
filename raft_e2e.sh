#!/usr/bin/env bash
set -euo pipefail

TS="$(date +%Y%m%d_%H%M%S)"
OUT="raft_test_${TS}.log"

N1="http://localhost:8000"
N2="http://localhost:8001"
N3="http://localhost:8002"

NET="$(docker network ls --format '{{.Name}}' | grep -E 'raft.*raft-net|raft-net|_raft-net' | head -n 1 || true)"
if [[ -z "$NET" ]]; then
  # fallback: choose the only network that contains raft-net substring
  NET="$(docker network ls --format '{{.Name}}' | grep -i 'raft-net' | head -n 1 || true)"
fi

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

req() {
  # req METHOD URL [JSON]
  local method="$1"; shift
  local url="$1"; shift
  if [[ $# -gt 0 ]]; then
    curl -sS -m 5 -X "$method" "$url" -H 'content-type: application/json' -d "$1"
  else
    curl -sS -m 5 -X "$method" "$url"
  fi
}

status_all() {
  log "STATUS node1: $(req GET "$N1/raft/status" || echo 'DOWN')"
  log "STATUS node2: $(req GET "$N2/raft/status" || echo 'DOWN')"
  log "STATUS node3: $(req GET "$N3/raft/status" || echo 'DOWN')"
}

find_leader_port() {
  for p in 8000 8001 8002; do
    local s
    s="$(curl -sS -m 2 "http://localhost:${p}/raft/status" || true)"
    if echo "$s" | grep -q '"role"[[:space:]]*:[[:space:]]*"LEADER"'; then
      echo "$p"; return 0
    fi
  done
  echo "" # unknown
}

container_by_port() {
  case "$1" in
    8000) echo "raft-node1" ;;
    8001) echo "raft-node2" ;;
    8002) echo "raft-node3" ;;
    *) echo "" ;;
  esac
}

wait_leader() {
  local tries=20
  while [[ $tries -gt 0 ]]; do
    local lp
    lp="$(find_leader_port)"
    if [[ -n "$lp" ]]; then
      echo "$lp"; return 0
    fi
    sleep 0.5
    tries=$((tries-1))
  done
  echo ""
}

clean_data_dirs() {
  log "Cleaning ./data/node{1,2,3} (optional but helps repeatability)"
  rm -rf ./data/node1/* ./data/node2/* ./data/node3/* 2>/dev/null || true
}

log "=== Raft E2E test start: $TS ==="
log "Compose up..."
docker compose up -d --build | tee -a "$OUT" >/dev/null
sleep 2
status_all

LEADER_PORT="$(wait_leader)"
if [[ -z "$LEADER_PORT" ]]; then
  log "ERROR: leader not elected."
  exit 1
fi
LEADER_URL="http://localhost:${LEADER_PORT}"
LEADER_CONT="$(container_by_port "$LEADER_PORT")"
log "Leader detected: port=$LEADER_PORT container=$LEADER_CONT"

log ""
log "=== Scenario 1: Basic KV PUT/GET/DELETE on leader ==="
req PUT "$LEADER_URL/kv/a" '{"value":"1"}' | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null
req GET "$LEADER_URL/kv/a" | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null
req DELETE "$LEADER_URL/kv/a" | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null
req GET "$LEADER_URL/kv/a" | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null

log ""
log "=== Scenario 2: Writes batch, kill leader, new leader, continue writes ==="
for i in $(seq 1 12); do
  req PUT "$LEADER_URL/kv/failover$i" "{\"value\":\"v$i\"}" >/dev/null || true
done
log "First batch written."

log "Killing leader hard: $LEADER_CONT"
docker kill -s KILL "$LEADER_CONT" | tee -a "$OUT" >/dev/null
sleep 2
status_all

NEW_LEADER_PORT="$(wait_leader)"
if [[ -z "$NEW_LEADER_PORT" ]]; then
  log "ERROR: no new leader after killing leader."
  exit 1
fi
NEW_LEADER_URL="http://localhost:${NEW_LEADER_PORT}"
NEW_LEADER_CONT="$(container_by_port "$NEW_LEADER_PORT")"
log "New leader detected: port=$NEW_LEADER_PORT container=$NEW_LEADER_CONT"

for i in $(seq 13 24); do
  req PUT "$NEW_LEADER_URL/kv/failover$i" "{\"value\":\"v$i\"}" >/dev/null || true
done
log "Second batch written on new leader."

log "Restarting old leader container..."
docker start "$LEADER_CONT" | tee -a "$OUT" >/dev/null
sleep 2
status_all

log ""
log "=== Scenario 3: Lose quorum (kill 2 nodes), verify no progress, restore quorum ==="
log "Killing node2 + node3..."
docker kill -s KILL raft-node2 | tee -a "$OUT" >/dev/null || true
docker kill -s KILL raft-node3 | tee -a "$OUT" >/dev/null || true
sleep 1
status_all

log "Attempt PUT with no quorum (should fail/timeout/not leader):"
( curl -i -sS -m 5 -X PUT "$N1/kv/noquorum" -H 'content-type: application/json' -d '{"value":"x"}' || true ) | tee -a "$OUT"

log "Restore one node to regain quorum..."
docker start raft-node2 | tee -a "$OUT" >/dev/null || true
sleep 2
status_all

LEADER_PORT="$(wait_leader)"
if [[ -z "$LEADER_PORT" ]]; then
  log "ERROR: leader not elected after quorum restore."
  exit 1
fi
LEADER_URL="http://localhost:${LEADER_PORT}"
log "Leader after restore: $LEADER_PORT"

log "PUT after quorum restore (should succeed on leader):"
req PUT "$LEADER_URL/kv/afterquorum" '{"value":"ok"}' | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null
req GET "$LEADER_URL/kv/afterquorum" | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null

log ""
log "=== Scenario 4: Partition 2–1 by network disconnect ==="
if [[ -z "$NET" ]]; then
  log "WARNING: Could not detect docker network name automatically; skipping partition scenario."
else
  log "Docker network detected: $NET"
  log "Disconnecting raft-node1 from $NET"
  docker network disconnect "$NET" raft-node1 | tee -a "$OUT" >/dev/null || true
  sleep 2
  status_all

  log "Try PUT to isolated node1 (should fail/not commit):"
  ( curl -i -sS -m 5 -X PUT "$N1/kv/isolated" -H 'content-type: application/json' -d '{"value":"bad"}' || true ) | tee -a "$OUT"

  log "Reconnect raft-node1 to $NET"
  docker network connect "$NET" raft-node1 | tee -a "$OUT" >/dev/null || true
  sleep 2
  status_all
fi

log ""
log "=== Scenario 5: Snapshot/compaction + lagging follower catch-up ==="
LEADER_PORT="$(wait_leader)"; LEADER_URL="http://localhost:${LEADER_PORT}"
log "Stopping raft-node3 to make it lag"
docker stop raft-node3 | tee -a "$OUT" >/dev/null || true
sleep 1

log "Write 12 keys (SNAPSHOT_THRESHOLD=3 should trigger snapshots/compaction)"
for i in $(seq 1 12); do
  req PUT "$LEADER_URL/kv/snap$i" "{\"value\":\"sv$i\"}" >/dev/null || true
done

log "Start raft-node3 and show last logs (look for snapshot/install_snapshot)"
docker start raft-node3 | tee -a "$OUT" >/dev/null || true
sleep 2
docker logs --tail 120 raft-node3 | tee -a "$OUT"

log ""
log "=== Scenario 6: Crash test (hard kill) and data survives after restart ==="
LEADER_PORT="$(wait_leader)"; LEADER_URL="http://localhost:${LEADER_PORT}"
LEADER_CONT="$(container_by_port "$LEADER_PORT")"
log "Leader now: $LEADER_PORT ($LEADER_CONT)"

req PUT "$LEADER_URL/kv/crashA" '{"value":"ok"}' | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null
log "Hard kill leader container to simulate crash: $LEADER_CONT"
docker kill -s KILL "$LEADER_CONT" | tee -a "$OUT" >/dev/null
sleep 1
docker start "$LEADER_CONT" | tee -a "$OUT" >/dev/null
sleep 2

LEADER_PORT="$(wait_leader)"
LEADER_URL="http://localhost:${LEADER_PORT}"
log "Leader after crash restart: $LEADER_PORT"
log "GET crashA (must be ok):"
req GET "$LEADER_URL/kv/crashA" | tee -a "$OUT"; echo | tee -a "$OUT" >/dev/null

log ""
log "=== DONE. Log saved to $OUT ==="
echo "$OUT"
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

import httpx

from src.app.raft.node import RaftNode, RaftRole

logger = logging.getLogger("raft")


def _serialize_entries(node: RaftNode, start_index: int) -> List[Dict]:
    """Сериализует log entries для AppendEntries."""
    out: List[Dict] = []
    for e in node.log.entries_from(start_index):
        out.append({"term": e.term, "command": e.command})
    return out


async def replicate_to_peer(
    *,
    client: httpx.AsyncClient,
    node: RaftNode,
    peer_id: str,
    base_url: str,
    leader_commit: int,
    max_backtracks: int = 20,
) -> bool:
    """Догоняющая репликация к peer через nextIndex/matchIndex."""
    async with node.lock:
        if node.role != RaftRole.LEADER:
            return False

        if peer_id not in node.next_index:
            node.next_index[peer_id] = node.log.last_index() + 1
        if peer_id not in node.match_index:
            node.match_index[peer_id] = 0

        op_term = node.current_term

    for _ in range(max_backtracks):
        async with node.lock:
            if node.role != RaftRole.LEADER or node.current_term != op_term:
                return False

            next_idx = max(1, node.next_index.get(peer_id, 1))
            need_snapshot = next_idx <= node.log.base_index

            prev_idx: int | None = None
            entries: List[Dict] = []
            payload: Dict[str, object] | None = None
            snap_payload: Dict[str, object] | None = None

            if need_snapshot:
                if node.snapshot_state is None:
                    return False

                snap_payload = {
                    "term": node.current_term,
                    "leader_id": node.node_id,
                    "last_included_index": node.log.base_index,
                    "last_included_term": node.log.base_term,
                    "state": node.snapshot_state,
                }
            else:
                prev_idx = next_idx - 1
                prev_term = node.log.term_at(prev_idx)
                entries = _serialize_entries(node, next_idx)

                payload = {
                    "term": node.current_term,
                    "leader_id": node.node_id,
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": leader_commit,
                }

        try:
            if need_snapshot:
                resp = await client.post(f"{base_url}/raft/install_snapshot", json=snap_payload)
            else:
                resp = await client.post(f"{base_url}/raft/append_entries", json=payload)

            if resp.status_code != 200:
                return False

            data = resp.json()
        except Exception as exc:
            logger.warning("[%s] replicate_to_peer(%s) failed: %r", node.node_id, peer_id, exc)
            return False

        async with node.lock:
            if node.role != RaftRole.LEADER or node.current_term != op_term:
                return False

            resp_term = int(data.get("term", op_term))
            success = bool(data.get("success", False))

            if resp_term > node.current_term:
                node.become_follower(resp_term)
                return False

            if need_snapshot:
                if success:
                    node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), node.log.base_index)
                    node.next_index[peer_id] = max(node.next_index.get(peer_id, 1), node.log.base_index + 1)
                    return True
                return False

            if success:
                if prev_idx is None:
                    return False
                if entries:
                    match = prev_idx + len(entries)
                    node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), match)
                    node.next_index[peer_id] = match + 1
                else:
                    node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), prev_idx)
                    node.next_index[peer_id] = max(node.next_index.get(peer_id, 1), prev_idx + 1)
                return True

            node.next_index[peer_id] = max(1, next_idx - 1)

        await asyncio.sleep(0)

    return False


def advance_commit_index(node: RaftNode, *, cluster_size: int | None = None) -> Optional[int]:
    """Продвигает commit_index по правилу RAFT."""
    if node.role != RaftRole.LEADER:
        return None

    last = node.log.last_index()
    cluster = node.get_cluster_nodes()
    majority = node.majority()

    for n in range(last, node.commit_index, -1):
        if node.log.term_at(n) != node.current_term:
            continue
        replicated = 0
        for nid in cluster:
            if nid == node.node_id:
                replicated += 1 if node.log.last_index() >= n else 0
            else:
                replicated += 1 if node.match_index.get(nid, 0) >= n else 0
        if replicated >= majority:
            node.commit_index = n
            return n

    return None
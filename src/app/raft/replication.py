from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

import httpx

from src.app.raft.node import RaftNode, RaftRole

logger = logging.getLogger("raft")


def _serialize_entries(node: RaftNode, start_index: int) -> List[Dict]:
    """Сериализует log entries начиная с start_index в payload для AppendEntries."""
    out: List[Dict] = []
    for e in node.log.entries_from(start_index):
        out.append({"term": e.term, "command": e.command})
    return out


async def _send_install_snapshot(
    *,
    client: httpx.AsyncClient,
    node: RaftNode,
    peer_id: str,
    base_url: str,
) -> bool:
    """
    Шлём snapshot на peer. Snapshot должен соответствовать node.log.base_index.
    """
    if node.snapshot_state is None:
        return False

    payload = {
        "term": node.current_term,
        "leader_id": node.node_id,
        "last_included_index": node.log.base_index,
        "last_included_term": node.log.base_term,
        "state": node.snapshot_state,
    }

    try:
        resp = await client.post(f"{base_url}/raft/install_snapshot", json=payload)
    except (httpx.HTTPError, OSError) as e:
        logger.warning(
            "install_snapshot to %s failed: %r",
            peer_id,
            e,
        )
        return False

    if resp.status_code != 200:
        return False

    data = resp.json()
    resp_term = int(data.get("term", node.current_term))
    success = bool(data.get("success", False))

    if resp_term > node.current_term:
        node.become_follower(resp_term)
        return False

    if success:
        node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), node.log.base_index)
        node.next_index[peer_id] = max(node.next_index.get(peer_id, 1), node.log.base_index + 1)
        return True

    return False


async def replicate_to_peer(
    *,
    client: httpx.AsyncClient,
    node: RaftNode,
    peer_id: str,
    base_url: str,
    leader_commit: int,
    max_backtracks: int = 20,
) -> bool:
    """
    Догоняющая репликация к peer через nextIndex/matchIndex.
    """
    if node.role != RaftRole.LEADER:
        return False

    if peer_id not in node.next_index:
        node.next_index[peer_id] = node.log.last_index() + 1
    if peer_id not in node.match_index:
        node.match_index[peer_id] = 0

    term = node.current_term

    for _ in range(max_backtracks):
        if node.role != RaftRole.LEADER or node.current_term != term:
            return False

        next_idx = max(1, node.next_index.get(peer_id, 1))
        if next_idx <= node.log.base_index:
            ok = await _send_install_snapshot(
                client=client,
                node=node,
                peer_id=peer_id,
                base_url=base_url,
            )
            if not ok:
                return False
            # после snapshot повторим цикл (теперь next_index должен быть base_index+1)
            await asyncio.sleep(0)
            continue
        prev_idx = next_idx - 1
        prev_term = node.log.term_at(prev_idx)
        entries = _serialize_entries(node, next_idx)

        payload = {
            "term": term,
            "leader_id": node.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": leader_commit,
        }

        try:
            resp = await client.post(f"{base_url}/raft/append_entries", json=payload)
            if resp.status_code != 200:
                return False

            data = resp.json()
            resp_term = int(data.get("term", term))
            success = bool(data.get("success", False))

            if resp_term > node.current_term:
                node.become_follower(resp_term)
                return False

            if success:
                if entries:
                    match = prev_idx + len(entries)
                    node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), match)
                    node.next_index[peer_id] = match + 1
                else:
                    node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), prev_idx)
                    node.next_index[peer_id] = max(node.next_index.get(peer_id, 1), prev_idx + 1)
                return True

            # success=false -> откатываем nextIndex и пробуем ещё
            node.next_index[peer_id] = max(1, next_idx - 1)
            await asyncio.sleep(0)

        except Exception as exc:
            logger.warning("[%s] replicate_to_peer(%s) failed: %r", node.node_id, peer_id, exc)
            return False

    return False


def advance_commit_index(node: RaftNode, *, cluster_size: int | None = None) -> Optional[int]:
    """Продвигает commit_index по правилу RAFT.

    Коммитим N, если:
      1) term(N) == currentTerm
      2) N реплицирован минимум на majority статического кластера.
    """
    if node.role != RaftRole.LEADER:
        return None

    last = node.log.last_index()
    cluster = node.cluster_nodes()
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
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
        # Нечего послать (не создавали snapshot) — значит это ошибка логики,
        # но мягко фейлим и дадим обычному backtracking продолжить.
        return False

    payload = {
        "term": node.current_term,
        "leader_id": node.node_id,
        "last_included_index": node.log.base_index,
        "last_included_term": node.log.base_term,
        "state": node.snapshot_state,
    }

    resp = await client.post(f"{base_url}/raft/install_snapshot", json=payload)
    if resp.status_code != 200:
        return False

    data = resp.json()
    resp_term = int(data.get("term", node.current_term))
    success = bool(data.get("success", False))

    if resp_term > node.current_term:
        node.become_follower(resp_term)
        return False

    if success:
        # peer гарантированно имеет state до base_index
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


def advance_commit_index(node: RaftNode, *, cluster_size: int) -> Optional[int]:
    """
    Продвигает commit_index по правилу RAFT:
    коммитим N, если N реплицирован на majority и term(N) == currentTerm.
    """
    if node.role != RaftRole.LEADER:
        return None

    majority = cluster_size // 2 + 1
    match_indexes = [node.log.last_index()] + [node.match_index.get(p, 0) for p in node.peers]
    match_indexes.sort(reverse=True)

    n = match_indexes[majority - 1] if len(match_indexes) >= majority else 0

    if n > node.commit_index and node.log.term_at(n) == node.current_term:
        node.commit_index = n
        return n
    return None
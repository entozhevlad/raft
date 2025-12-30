from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

import httpx
from fastapi import HTTPException, Request

from src.app.raft.log import LogEntry
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.persistence import save_full_state
from src.app.raft.replication import advance_commit_index, replicate_to_peer
from src.app.utils import errors


def get_raft_node(request: Request) -> RaftNode:
    raft_node: RaftNode = request.app.state.raft_node
    return raft_node


async def _replicate_command(
    request: Request,
    command: Dict[str, Any],
    *,
    commit_timeout_s: float = 3.0,
) -> int:
    node = get_raft_node(request)
    peer_addresses: Dict[str, str] = request.app.state.peer_addresses
    node_data_dir: str = request.app.state.data_dir

    async with node.lock:
        if node.role != RaftRole.LEADER:
            raise RuntimeError("replicate_command called on non-leader")

        new_index = node.log.last_index() + 1
        entry = LogEntry(term=node.current_term, index=new_index, command=command)
        node.log.append([entry])
        save_full_state(node, node_data_dir)

        op_term = node.current_term

    deadline = time.monotonic() + commit_timeout_s

    async with httpx.AsyncClient(timeout=1.0) as client:
        while time.monotonic() < deadline:
            async with node.lock:
                if node.role != RaftRole.LEADER or node.current_term != op_term:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "error": "not_leader",
                            "node_id": node.node_id,
                            "role": node.role.name,
                            "leader_id": node.leader_id,
                        },
                    )
                leader_commit = node.commit_index

            # репликация по сети без lock
            for peer_id, base_url in peer_addresses.items():
                await replicate_to_peer(
                    client=client,
                    node=node,
                    peer_id=peer_id,
                    base_url=base_url,
                    leader_commit=leader_commit,
                )

            # commit/apply под lock
            async with node.lock:
                if node.role != RaftRole.LEADER or node.current_term != op_term:
                    continue

                advanced = advance_commit_index(node)
                if advanced is not None:
                    node.apply_committed_entries()
                    save_full_state(node, node_data_dir)

                if node.commit_index >= new_index:
                    return new_index

            await asyncio.sleep(0.05)

    raise HTTPException(
        status_code=503,
        detail={
            "error": "commit_timeout",
            "message": "Command was not committed by majority within timeout",
            "node_id": node.node_id,
            "term": node.current_term,
            "log_index": new_index,
        },
    )

async def _confirm_leader_quorum(
    *,
    node: RaftNode,
    peer_addresses: Dict[str, str],
    timeout_s: float = 1.0,
    client: httpx.AsyncClient | None = None,
) -> None:
    """Подтверждение лидерского кворума."""
    if node.role != RaftRole.LEADER:
        raise HTTPException(
            status_code=409,
            detail=errors.not_leader(
                node_id=node.node_id,
                role=node.role.name,
                leader_id=node.leader_id,
            ),
        )

    if len(node.get_cluster_nodes()) == 1:
        return
    majority = node.majority()
    term = node.current_term

    prev_idx = node.log.last_index()
    prev_term = node.log.term_at(prev_idx)
    payload = {
        "term": term,
        "leader_id": node.node_id,
        "prev_log_index": prev_idx,
        "prev_log_term": prev_term,
        "entries": [],
        "leader_commit": node.commit_index,
    }

    async def _ping_peer(peer_id: str, base_url: str) -> bool:
        try:
            resp = await client.post(f"{base_url}/raft/append_entries", json=payload)
        except (httpx.HTTPError, OSError):
            return False

        if resp.status_code != 200:
            return False

        data = resp.json()
        resp_term = int(data.get("term", term))
        success = bool(data.get("success", False))

        if resp_term > node.current_term:
            node.become_follower(resp_term)
            return False

        if node.current_term != term or node.role != RaftRole.LEADER:
            return False

        return success

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout_s)

    ok = 1
    try:
        tasks = [
            asyncio.create_task(_ping_peer(peer_id, base_url))
            for peer_id, base_url in peer_addresses.items()
        ]
        done, pending = await asyncio.wait(tasks, timeout=timeout_s)

        for t in pending:
            t.cancel()

        for t in done:
            try:
                if t.result():
                    ok += 1
            except Exception:
                pass
    finally:
        if owns_client and client is not None:
            await client.aclose()

    if node.role != RaftRole.LEADER or node.current_term != term:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "stale_leader",
                "node_id": node.node_id,
                "role": node.role.name,
                "leader_id": node.leader_id,
                "term": node.current_term,
            },
        )

    if ok < majority:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "read_quorum_failed",
                "message": "Failed to confirm leadership on majority for linearizable read",
                "node_id": node.node_id,
                "term": node.current_term,
                "ok": ok,
                "majority": majority,
            },
        )


async def _ensure_committed_in_current_term(request: Request, *, timeout_s: float = 2.0) -> None:
    """Подтверждение лидера."""

    node = get_raft_node(request)
    peer_addresses: Dict[str, str] = request.app.state.peer_addresses
    if node.role != RaftRole.LEADER or len(node.get_cluster_nodes()) == 1:
        return

    if node.commit_index == 0 or node.log.term_at(node.commit_index) != node.current_term:
        await _replicate_command(
            request,
            {"op": "noop"},
            commit_timeout_s=timeout_s,
        )
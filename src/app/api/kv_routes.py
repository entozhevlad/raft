from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.app.raft.log import LogEntry
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.persistence import save_full_state, save_metadata
from src.app.raft.replication import advance_commit_index, replicate_to_peer

router = APIRouter(tags=["kv"])


def get_raft_node(request: Request) -> RaftNode:
    raft_node: RaftNode = request.app.state.raft_node  # type: ignore[assignment]
    return raft_node


class PutRequest(BaseModel):
    value: Any


async def _replicate_command(
    request: Request,
    command: Dict[str, Any],
    *,
    commit_timeout_s: float = 3.0,
) -> int:
    """Репликация одной команды через RAFT (только на лидере).

    Алгоритм:
      1) append в локальный лог
      2) fsync-подобно: сохраняем метаданные/лог на диск до коммита (минимально)
      3) догоняющая репликация на peers через nextIndex/matchIndex
      4) двигаем commitIndex по правилу RAFT
      5) apply + save_full_state() после коммита

    Возвращает индекс записи в журнале.
    """
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        raise RuntimeError("replicate_command called on non-leader")

    peer_addresses: Dict[str, str] = request.app.state.peer_addresses
    node_data_dir: str = request.app.state.data_dir

    # 1) Append локально
    new_index = node.log.last_index() + 1
    entry = LogEntry(term=node.current_term, index=new_index, command=command)
    node.log.append([entry])

    # Важно: term/voted_for и лог должны пережить рестарт (упрощенно — сохраняем целиком).
    save_metadata(node, node_data_dir)
    save_full_state(node, node_data_dir)

    cluster_size = 1 + len(peer_addresses)

    deadline = time.monotonic() + commit_timeout_s

    async with httpx.AsyncClient(timeout=1.0) as client:
        while time.monotonic() < deadline:
            # Если лидерство потеряно — прекращаем.
            if node.role != RaftRole.LEADER:
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

            # 2) Догоняющая репликация на всех peers.
            for peer_id, base_url in peer_addresses.items():
                await replicate_to_peer(
                    client=client,
                    node=node,
                    peer_id=peer_id,
                    base_url=base_url,
                    leader_commit=leader_commit,
                )

            # 3) Пробуем продвинуть commit_index.
            advanced = advance_commit_index(node, cluster_size=cluster_size)
            if advanced is not None:
                node.apply_committed_entries()
                save_full_state(node, node_data_dir)

            # 4) Если наша запись закоммичена — успех.
            if node.commit_index >= new_index:
                return new_index

            await asyncio.sleep(0.05)

    # Таймаут коммита: кворум/сеть/падения
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


@router.put("/{key}")
async def put_value(request: Request, key: str, body: PutRequest) -> Dict[str, Any]:
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        detail: Dict[str, Any] = {
            "error": "not_leader",
            "node_id": node.node_id,
            "role": node.role.name,
            "leader_id": node.leader_id,
        }
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses
        if node.leader_id and node.leader_id in peer_addresses:
            detail["leader_address"] = peer_addresses[node.leader_id]

        raise HTTPException(status_code=409, detail=detail)

    command = {"op": "put", "key": key, "value": body.value}
    log_index = await _replicate_command(request, command)

    return {
        "status": "ok",
        "op": "put",
        "key": key,
        "value": body.value,
        "log_index": log_index,
        "term": node.current_term,
        "node_id": node.node_id,
    }

async def _confirm_leader_quorum(
    *,
    node: RaftNode,
    peer_addresses: Dict[str, str],
    timeout_s: float = 1.0,
    client: httpx.AsyncClient | None = None,
) -> None:
    """ReadIndex-подобный барьер: подтверждаем, что мы всё ещё лидер на кворуме.

    Идея:
      - лидер посылает heartbeat (AppendEntries без entries) всем peer'ам
      - ждём ответы большинства (success=True) в текущем term
      - если кто-то отвечает term > current_term -> немедленно step down
    """
    if node.role != RaftRole.LEADER:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "not_leader",
                "node_id": node.node_id,
                "role": node.role.name,
                "leader_id": node.leader_id,
            },
        )

    cluster_size = 1 + len(peer_addresses)
    if cluster_size == 1:
        return

    majority = cluster_size // 2 + 1
    term = node.current_term

    # Heartbeat payload (без репликации лога)
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

        # term и роль должны совпадать с началом барьера
        if node.current_term != term or node.role != RaftRole.LEADER:
            return False

        return success

    owns_client = client is None
    if owns_client:
        client = httpx.AsyncClient(timeout=timeout_s)

    ok = 1  # self
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

    # Если во время проверки выяснилось, что term вырос/роль сменилась — это stale leader
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
    """Лениво гарантируем commit записи в текущем term (no-op), как часто делают после election.

    Это помогает сделать ReadIndex безопасным сразу после лидерства, не меняя фоновые таски.
    """
    node = get_raft_node(request)
    peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
    cluster_size = 1 + len(peer_addresses)

    if node.role != RaftRole.LEADER or cluster_size == 1:
        return

    if node.commit_index == 0 or node.log.term_at(node.commit_index) != node.current_term:
        # no-op команда (state_machine её проигнорирует)
        await _replicate_command(
            request,
            {"op": "noop"},
            commit_timeout_s=timeout_s,
        )

@router.get("/{key}")
async def get_value(request: Request, key: str) -> Dict[str, Any]:
    node = get_raft_node(request)

    # Линейризуемые чтения: только лидер + защита от stale leader.
    if node.role != RaftRole.LEADER:
        detail: Dict[str, Any] = {
            "error": "not_leader",
            "node_id": node.node_id,
            "role": node.role.name,
            "leader_id": node.leader_id,
        }
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
        if node.leader_id and node.leader_id in peer_addresses:
            detail["leader_address"] = peer_addresses[node.leader_id]
        raise HTTPException(status_code=409, detail=detail)

    # (0) Гарантируем commit в текущем term (лениво, через noop)
    await _ensure_committed_in_current_term(request)

    # (1) ReadIndex barrier: подтверждаем лидерство на majority
    peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
    await _confirm_leader_quorum(node=node, peer_addresses=peer_addresses)

    # (2) Доприменяем всё закоммиченное перед чтением
    node.apply_committed_entries()

    value = node.state_machine.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail={"error": "key_not_found", "key": key})

    return {
        "key": key,
        "value": value,
        "node_id": node.node_id,
        "role": node.role.name,
        "leader_id": node.leader_id,
    }


@router.delete("/{key}")
async def delete_value(request: Request, key: str) -> Dict[str, Any]:
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        detail: Dict[str, Any] = {
            "error": "not_leader",
            "node_id": node.node_id,
            "role": node.role.name,
            "leader_id": node.leader_id,
        }
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
        if node.leader_id and node.leader_id in peer_addresses:
            detail["leader_address"] = peer_addresses[node.leader_id]

        raise HTTPException(status_code=409, detail=detail)

    command = {"op": "delete", "key": key}
    log_index = await _replicate_command(request, command)

    return {
        "status": "ok",
        "op": "delete",
        "key": key,
        "log_index": log_index,
        "term": node.current_term,
        "node_id": node.node_id,
    }
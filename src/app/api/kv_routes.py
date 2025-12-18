# src/app/api/kv_routes.py
#
# KV-API поверх RAFT:
#   - PUT    /kv/{key}
#   - GET    /kv/{key}
#   - DELETE /kv/{key}
#
# PUT/DELETE принимаются только лидером.
# Лидер записывает команду в журнал, реплицирует её (с догоняющей репликацией)
# и коммитит только когда запись подтверждена большинством.

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
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses  # type: ignore[assignment]
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


@router.get("/{key}")
async def get_value(request: Request, key: str) -> Dict[str, Any]:
    node = get_raft_node(request)

    # Упрощение для консистентности: читаем только на лидере.
    # Если хочешь stale reads — можно убрать этот блок.
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
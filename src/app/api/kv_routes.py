# src/app/api/kv_routes.py
#
# KV-API поверх RAFT:
#   - PUT    /kv/{key}
#   - GET    /kv/{key}
#   - DELETE /kv/{key}
#
# PUT/DELETE принимаются только лидером.
# Лидер записывает команду в журнал, реплицирует её и при коммите
# сохраняет состояние на диск.

from __future__ import annotations

from typing import Any, Dict

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.app.raft.log import LogEntry
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.persistence import save_full_state

router = APIRouter(tags=["kv"])


def get_raft_node(request: Request) -> RaftNode:
    raft_node: RaftNode = request.app.state.raft_node  # type: ignore[assignment]
    return raft_node


class PutRequest(BaseModel):
    value: Any


async def _replicate_command(request: Request, command: Dict[str, Any]) -> int:
    """
    Репликация одной команды через RAFT.
    Вызывается только на лидере.

    Возвращает индекс записи в журнале.
    """
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        raise RuntimeError("replicate_command called on non-leader")

    peer_addresses: Dict[str, str] = request.app.state.peer_addresses
    node_data_dir: str = request.app.state.data_dir

    # 1. Добавляем запись в локальный журнал.
    new_index = node.log.last_index() + 1
    entry = LogEntry(
        term=node.current_term,
        index=new_index,
        command=command,
    )
    node.log.append([entry])

    term = node.current_term
    prev_log_index = new_index - 1
    prev_log_term = node.log.term_at(prev_log_index)
    leader_commit = node.commit_index

    successes = 1  # сам лидер
    total_nodes = 1 + len(peer_addresses)
    majority = total_nodes // 2 + 1

    async with httpx.AsyncClient(timeout=1.0) as client:
        for peer_id, base_url in peer_addresses.items():
            payload = {
                "term": term,
                "leader_id": node.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [command],
                "leader_commit": leader_commit,
            }

            try:
                resp = await client.post(
                    f"{base_url}/raft/append_entries",
                    json=payload,
                )

                if resp.status_code != 200:
                    continue

                data = resp.json()
                resp_term = int(data.get("term", term))
                success = bool(data.get("success", False))

                if resp_term > node.current_term:
                    node.become_follower(resp_term)
                    break

                if success:
                    successes += 1

            except Exception:
                continue

    # 3. Если получили большинство — коммитим и сохраняем на диск.
    if successes >= majority:
        node.commit_index = new_index
        node.apply_committed_entries()
        save_full_state(node, node_data_dir)

    return new_index


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
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses
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

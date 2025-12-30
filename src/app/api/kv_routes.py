from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from src.app.utils.deps import (_confirm_leader_quorum,
                            _ensure_committed_in_current_term,
                            _replicate_command, get_raft_node)
from src.app.raft.models import PutRequest
from src.app.raft.node import RaftRole
from src.app.utils import errors

router = APIRouter(tags=["kv"])


@router.put("/{key}")
async def put_value(request: Request, key: str, body: PutRequest) -> Dict[str, Any]:
    """Добавить данные в БД."""
    node = get_raft_node(request)

    if node.role != RaftRole.LEADER:
        peer_addresses: Dict[str, str] = request.app.state.peer_addresses
        leader_addr = (
            peer_addresses[node.leader_id] if node.leader_id and node.leader_id in peer_addresses else None
        )
        raise HTTPException(
            status_code=409,
            detail=errors.not_leader(
                node_id=node.node_id,
                role=node.role.name,
                leader_id=node.leader_id,
                leader_address=leader_addr,
            ),
        )

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

    await _ensure_committed_in_current_term(request)
    peer_addresses: Dict[str, str] = request.app.state.peer_addresses
    await _confirm_leader_quorum(node=node, peer_addresses=peer_addresses)

    async with node.lock:
        node.apply_committed_entries()
        value = node.state_machine.get(key)

    if value is None:
        raise HTTPException(status_code=404, detail=errors.key_not_found(key))

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

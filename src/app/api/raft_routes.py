from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request

from src.app.api.kv_routes import _replicate_command
from src.app.raft.node import RaftNode, RaftRole
from src.app.raft.persistence import save_full_state

router = APIRouter(tags=["raft"])
logger = logging.getLogger("raft")


def get_raft_node(request: Request) -> RaftNode:
    raft_node: RaftNode = request.app.state.raft_node
    return raft_node


@router.post("/request_vote")
async def request_vote(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    node = get_raft_node(request)

    try:
        term = int(body.get("term", 0))
        candidate_id = str(body.get("candidate_id"))
        last_log_index = int(body.get("last_log_index", 0))
        last_log_term = int(body.get("last_log_term", 0))

        logger.info(
            "[%s] HTTP /raft/request_vote: term=%s cand=%s last_idx=%s last_term=%s",
            node.node_id,
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        )

        resp_term, vote_granted = node.handle_request_vote(
            term=term,
            candidate_id=candidate_id,
            candidate_last_log_index=last_log_index,
            candidate_last_log_term=last_log_term,
        )

        # metadata могла измениться (current_term, voted_for) — сохраним
        node_data_dir: str = request.app.state.data_dir
        save_full_state(node, node_data_dir)

        return {"term": resp_term, "vote_granted": vote_granted}

    except Exception as exc:
        logger.exception(
            "[%s] ERROR in /raft/request_vote handler: %r",
            node.node_id,
            exc,
        )
        return {"term": node.current_term, "vote_granted": False}


@router.post("/append_entries")
async def append_entries(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    node = get_raft_node(request)

    try:
        term = int(body.get("term", 0))
        leader_id = str(body.get("leader_id"))
        prev_log_index = int(body.get("prev_log_index", 0))
        prev_log_term = int(body.get("prev_log_term", 0))
        entries = body.get("entries") or []
        leader_commit = int(body.get("leader_commit", 0))

        logger.info(
            "[%s] HTTP /raft/append_entries: from leader=%s term=%s entries=%d",
            node.node_id,
            leader_id,
            term,
            len(entries),
        )

        resp_term, success = node.handle_append_entries(
            term=term,
            leader_id=leader_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=leader_commit,
        )

        node.apply_committed_entries()

        # Журнал и KV могли измениться — сохраняем
        node_data_dir: str = request.app.state.data_dir
        save_full_state(node, node_data_dir)

        return {"term": resp_term, "success": success}

    except Exception as exc:
        logger.exception(
            "[%s] ERROR in /raft/append_entries handler: %r",
            node.node_id,
            exc,
        )
        return {"term": node.current_term, "success": False}


@router.get("/status")
async def raft_status(request: Request) -> Dict[str, Any]:
    node = get_raft_node(request)
    return {
        "node_id": node.node_id,
        "role": node.role.name,
        "term": node.current_term,
        "leader_id": node.leader_id,
        "commit_index": node.commit_index,
        "last_applied": node.last_applied,
        "last_log_index": node.log.last_index(),
        "last_log_term": node.log.last_term(),
    }

@router.post("/install_snapshot")
async def install_snapshot(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    node = get_raft_node(request)

    try:
        term = int(body.get("term", 0))
        leader_id = str(body.get("leader_id"))
        last_included_index = int(body.get("last_included_index", 0))
        last_included_term = int(body.get("last_included_term", 0))
        state = body.get("state") or {}

        resp_term, success = node.handle_install_snapshot(
            term=term,
            leader_id=leader_id,
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            state=state if isinstance(state, dict) else {},
        )

        node_data_dir: str = request.app.state.data_dir
        save_full_state(node, node_data_dir)

        return {"term": resp_term, "success": success}

    except Exception as exc:
        logger.exception("[%s] ERROR in /raft/install_snapshot handler: %r", node.node_id, exc)
        return {"term": node.current_term, "success": False}

@router.post("/members/add")
async def add_member(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    """Добавить voting member через joint consensus."""
    node = get_raft_node(request)
    if node.role != RaftRole.LEADER:
        raise HTTPException(
            status_code=409,
            detail={"error": "not_leader", "node_id": node.node_id, "role": node.role.name, "leader_id": node.leader_id},
        )

    if node.is_joint():
        raise HTTPException(status_code=409, detail={"error": "reconfiguration_in_progress"})

    new_id = str(body.get("node_id") or "").strip()
    addr = str(body.get("address") or "").strip()
    if not new_id or new_id == node.node_id:
        raise HTTPException(status_code=400, detail={"error": "invalid_node_id"})
    if not addr:
        raise HTTPException(status_code=400, detail={"error": "invalid_address"})

    old = set(node.members) if node.members else ({node.node_id} | set(node.peers))
    if new_id in old:
        return {"ok": True, "message": "already_member", "members": sorted(list(old))}

    new = set(old) | {new_id}

    # адрес нужен ДО старта joint-фазы (для репликации на новый узел)
    request.app.state.peer_addresses[new_id] = addr
    node.peer_addresses[new_id] = addr

    joint_cmd = {
        "op": "config",
        "phase": "joint",
        "old": sorted(list(old)),
        "new": sorted(list(new)),
        "peer_addresses": {new_id: addr},
    }
    final_cmd = {
        "op": "config",
        "phase": "final",
        "members": sorted(list(new)),
        "peer_addresses": dict(request.app.state.peer_addresses),
    }

    await _replicate_command(request, joint_cmd, commit_timeout_s=5.0)
    await _replicate_command(request, final_cmd, commit_timeout_s=5.0)

    return {"ok": True, "members": sorted(list(node.voting_members())), "joint": node.is_joint()}


@router.post("/members/remove")
async def remove_member(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    """Удалить voting member через joint consensus (упрощённо: одна операция за раз)."""
    node = get_raft_node(request)
    if node.role != RaftRole.LEADER:
        raise HTTPException(
            status_code=409,
            detail={"error": "not_leader", "node_id": node.node_id, "role": node.role.name, "leader_id": node.leader_id},
        )

    if node.is_joint():
        raise HTTPException(status_code=409, detail={"error": "reconfiguration_in_progress"})

    rm_id = str(body.get("node_id") or "").strip()
    if not rm_id or rm_id == node.node_id:
        raise HTTPException(status_code=400, detail={"error": "invalid_node_id"})

    old = set(node.members) if node.members else ({node.node_id} | set(node.peers))
    if rm_id not in old:
        return {"ok": True, "message": "not_a_member", "members": sorted(list(old))}

    new = set(old)
    new.remove(rm_id)

    joint_cmd = {
        "op": "config",
        "phase": "joint",
        "old": sorted(list(old)),
        "new": sorted(list(new)),
        "peer_addresses": dict(request.app.state.peer_addresses),
    }
    final_cmd = {
        "op": "config",
        "phase": "final",
        "members": sorted(list(new)),
        "peer_addresses": dict(request.app.state.peer_addresses),
    }

    await _replicate_command(request, joint_cmd, commit_timeout_s=5.0)
    await _replicate_command(request, final_cmd, commit_timeout_s=5.0)

    request.app.state.peer_addresses.pop(rm_id, None)
    node.peer_addresses.pop(rm_id, None)

    return {"ok": True, "members": sorted(list(node.voting_members())), "joint": node.is_joint()}
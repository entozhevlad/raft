from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Request

from src.app.raft.node import RaftNode
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
        async with node.lock:
            resp_term, vote_granted = node.handle_request_vote(
                term=term,
                candidate_id=candidate_id,
                candidate_last_log_index=last_log_index,
                candidate_last_log_term=last_log_term,
            )

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
        async with node.lock:
            resp_term, success = node.handle_append_entries(
                term=term,
                leader_id=leader_id,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries,
                leader_commit=leader_commit,
            )

            node.apply_committed_entries()

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
    return node.to_status_dict()

@router.post("/install_snapshot")
async def install_snapshot(request: Request, body: Dict[str, Any]) -> Dict[str, Any]:
    node = get_raft_node(request)

    try:
        term = int(body.get("term", 0))
        leader_id = str(body.get("leader_id"))
        last_included_index = int(body.get("last_included_index", 0))
        last_included_term = int(body.get("last_included_term", 0))
        state = body.get("state") or {}
        async with node.lock:
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

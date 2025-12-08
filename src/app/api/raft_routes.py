# src/app/api/raft_routes.py
#
# HTTP-обёртки над RAFT RPC:
#   - POST /raft/request_vote
#   - POST /raft/append_entries
#   - GET  /raft/status

from __future__ import annotations

import logging

from fastapi import APIRouter, Request

from src.app.raft.models import (
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from src.app.raft.node import RaftNode

router = APIRouter(tags=["raft"])
logger = logging.getLogger("raft")


def get_raft_node(request: Request) -> RaftNode:
    raft_node: RaftNode = request.app.state.raft_node  # type: ignore[assignment]
    return raft_node


async def request_vote(
    request: Request,
    body: RequestVoteRequest,
) -> RequestVoteResponse:
    """
    Обработчик RPC RequestVote.
    ВАЖНО: никогда не кидает исключения наружу, всегда возвращает 200.
    """
    node = get_raft_node(request)

    try:
        logger.info(
            "[%s] HTTP /raft/request_vote: term=%s cand=%s last_idx=%s last_term=%s",
            node.node_id,
            body.term,
            body.candidate_id,
            body.last_log_index,
            body.last_log_term,
        )

        term, vote_granted = node.handle_request_vote(
            term=body.term,
            candidate_id=body.candidate_id,
            candidate_last_log_index=body.last_log_index,
            candidate_last_log_term=body.last_log_term,
        )

        return RequestVoteResponse(term=term, vote_granted=vote_granted)

    except Exception as exc:
        # На всякий пожарный: если что-то пошло не так,
        # не роняем приложение и не даём 503.
        logger.exception(
            "[%s] ERROR in /raft/request_vote handler: %r",
            node.node_id,
            exc,
        )
        return RequestVoteResponse(term=node.current_term, vote_granted=False)


async def append_entries(
    request: Request,
    body: AppendEntriesRequest,
) -> AppendEntriesResponse:
    """
    Обработчик RPC AppendEntries (heartbeat + репликация).
    Также гарантированно возвращает 200.
    """
    node = get_raft_node(request)

    try:
        logger.info(
            "[%s] HTTP /raft/append_entries: from leader=%s term=%s entries=%d",
            node.node_id,
            body.leader_id,
            body.term,
            len(body.entries),
        )

        term, success = node.handle_append_entries(
            term=body.term,
            leader_id=body.leader_id,
            prev_log_index=body.prev_log_index,
            prev_log_term=body.prev_log_term,
            entries=body.entries,
            leader_commit=body.leader_commit,
        )

        node.apply_committed_entries()

        return AppendEntriesResponse(term=term, success=success)

    except Exception as exc:
        logger.exception(
            "[%s] ERROR in /raft/append_entries handler: %r",
            node.node_id,
            exc,
        )
        return AppendEntriesResponse(term=node.current_term, success=False)


async def raft_status(request: Request) -> dict:
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


router.add_api_route(
    path="/request_vote",
    endpoint=request_vote,
    methods=["POST"],
    summary="RAFT RequestVote RPC",
    response_model=RequestVoteResponse,
)

router.add_api_route(
    path="/append_entries",
    endpoint=append_entries,
    methods=["POST"],
    summary="RAFT AppendEntries RPC",
    response_model=AppendEntriesResponse,
)

router.add_api_route(
    path="/status",
    endpoint=raft_status,
    methods=["GET"],
    summary="RAFT node status",
)

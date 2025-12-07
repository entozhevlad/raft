# app/api/raft_routes.py
#
# HTTP-обёртки над RAFT RPC:
#   - POST /raft/request_vote
#   - POST /raft/append_entries
#
# ВАЖНО:
#   Здесь мы не реализуем таймеры и отправку RPC другим узлам.
#   Только приём запросов от других узлов и делегирование в RaftNode.

from __future__ import annotations

from fastapi import APIRouter, Request

from src.app.raft.models import (
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)
from src.app.raft.node import RaftNode

router = APIRouter(tags=["raft"])


def get_raft_node(request: Request) -> RaftNode:
    """
    Вспомогательная функция: достаёт RaftNode из state приложения.
    Мы кладём его туда в app.main:create_app().
    """
    raft_node: RaftNode = request.app.state.raft_node  # type: ignore[assignment]
    return raft_node


# === HANDLERS ===

async def request_vote(
    request: Request,
    body: RequestVoteRequest,
) -> RequestVoteResponse:
    """
    Обработчик RPC RequestVote.

    Вызывается другими узлами кластера во время выборов лидера.
    """
    node = get_raft_node(request)

    term, vote_granted = node.handle_request_vote(
        term=body.term,
        candidate_id=body.candidate_id,
        candidate_last_log_index=body.last_log_index,
        candidate_last_log_term=body.last_log_term,
    )

    return RequestVoteResponse(term=term, vote_granted=vote_granted)


async def append_entries(
    request: Request,
    body: AppendEntriesRequest,
) -> AppendEntriesResponse:
    """
    Обработчик RPC AppendEntries.

    Лидер присылает сюда heartbeat'ы и новые записи журнала.
    """
    node = get_raft_node(request)

    term, success = node.handle_append_entries(
        term=body.term,
        leader_id=body.leader_id,
        prev_log_index=body.prev_log_index,
        prev_log_term=body.prev_log_term,
        entries=body.entries,
        leader_commit=body.leader_commit,
    )

    # После обновления commitIndex узел должен применить закоммиченные записи
    # к state machine.
    node.apply_committed_entries()

    return AppendEntriesResponse(term=term, success=success)


# === РЕГИСТРАЦИЯ МАРШРУТОВ ===

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

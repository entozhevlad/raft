# app/raft/models.py
#
# Pydantic-модели для RAFT RPC:
#   - RequestVote
#   - AppendEntries
#
# Эти модели используются только для сетевого слоя (FastAPI),
# а внутри RaftNode мы работаем с "простыми" питоновскими типами.

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel


class RequestVoteRequest(BaseModel):
    """
    Запрос на голос на выборах лидера.
    Полностью соответствует описанию из RAFT.
    """
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteResponse(BaseModel):
    """
    Ответ на запрос голоса.
    """
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    """
    Запрос AppendEntries (используется и как heartbeat, и как репликация лога).

    entries:
      список команд (dict). Мы не навязываем тип команд,
      но в нашем KV-хранилище это будут dict вида:
        {"op": "put"|"delete", "key": str, "value": Any?}
    """
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    """
    Ответ на AppendEntries.
    """
    term: int
    success: bool

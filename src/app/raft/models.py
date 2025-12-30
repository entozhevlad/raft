from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel


class RequestVoteRequest(BaseModel):
    """Запрос на голос на выборах лидера."""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class RequestVoteResponse(BaseModel):
    """Ответ на запрос голоса."""

    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    """Запрос AppendEntries."""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]]
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    """Ответ на AppendEntries."""

    term: int
    success: bool

class PutRequest(BaseModel):
    value: Any
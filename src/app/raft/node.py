# app/raft/node.py
#
# Основной класс RaftNode: хранит состояние узла и реализует
# ядро алгоритма RAFT (без сетевого слоя и персистентности).
#
# Сейчас есть:
#   - роли узла (Follower / Candidate / Leader)
#   - структура состояния (currentTerm, votedFor, log, commitIndex, lastApplied)
#   - обработка RequestVote и AppendEntries
#   - применение закоммиченных записей к KV state machine

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from src.app.raft.log import LogEntry, RaftLog
from src.app.raft.state_machine import KeyValueStateMachine

logger = logging.getLogger("raft")


class RaftRole(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


@dataclass
class RaftNode:
    """
    Реализация одного RAFT-узла (без сетевого слоя).

    node_id  - уникальный идентификатор узла (строка)
    peers    - список идентификаторов других узлов

    Внутри храним:
      - current_term
      - voted_for
      - журнал
      - commit_index / last_applied
      - роль и leader_id
      - KV state machine
    """

    node_id: str
    peers: List[str]

    # --- Персистентное состояние (по RAFT) ---
    current_term: int = 0
    voted_for: Optional[str] = None
    log: RaftLog = field(default_factory=RaftLog)

    # --- Volatile state ---
    commit_index: int = 0
    last_applied: int = 0

    # --- Дополнительное состояние ---
    role: RaftRole = RaftRole.FOLLOWER
    leader_id: Optional[str] = None

    state_machine: KeyValueStateMachine = field(default_factory=KeyValueStateMachine)

    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    last_heartbeat_ts: float = field(default_factory=time.monotonic)

    # ================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==================

    def last_log_index_term(self) -> Tuple[int, int]:
        """Возвращает (lastLogIndex, lastLogTerm)."""
        return self.log.last_index(), self.log.last_term()

    # ================== ПЕРЕХОДЫ МЕЖДУ РОЛЯМИ ==================

    def become_follower(self, new_term: int, leader_id: Optional[str] = None) -> None:
        logger.info(
            "[%s] become FOLLOWER: term %s -> %s, leader=%s",
            self.node_id,
            self.current_term,
            new_term,
            leader_id,
        )
        self.role = RaftRole.FOLLOWER
        self.current_term = new_term
        self.leader_id = leader_id
        self.voted_for = None
        self.last_heartbeat_ts = time.monotonic()

    def become_candidate(self) -> None:
        self.current_term += 1
        self.role = RaftRole.CANDIDATE
        self.voted_for = self.node_id
        self.last_heartbeat_ts = time.monotonic()
        logger.info(
            "[%s] become CANDIDATE term=%s",
            self.node_id,
            self.current_term,
        )

    def become_leader(self) -> None:
        logger.info(
            "[%s] become LEADER term=%s",
            self.node_id,
            self.current_term,
        )
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id
        self.last_heartbeat_ts = time.monotonic()

        last_index = self.log.last_index()
        self.next_index = {peer_id: last_index + 1 for peer_id in self.peers}
        self.match_index = {peer_id: 0 for peer_id in self.peers}

    # ================== REQUESTVOTE ==================

    def handle_request_vote(
        self,
        term: int,
        candidate_id: str,
        candidate_last_log_index: int,
        candidate_last_log_term: int,
    ) -> Tuple[int, bool]:
        """
        Обработка RPC RequestVote (без сетевого слоя).

        Возвращает (currentTerm, voteGranted).
        """
        logger.info(
            "[%s] handle_request_vote: term=%s, candidate=%s, my_term=%s",
            self.node_id,
            term,
            candidate_id,
            self.current_term,
        )

        # 1. Устаревший term
        if term < self.current_term:
            logger.info("[%s] reject vote for %s: stale term", self.node_id, candidate_id)
            return self.current_term, False

        # 2. Новый term
        if term > self.current_term:
            self.become_follower(term)

        # Теперь term == current_term
        if self.voted_for is not None and self.voted_for != candidate_id:
            logger.info(
                "[%s] reject vote for %s: already voted for %s",
                self.node_id,
                candidate_id,
                self.voted_for,
            )
            return self.current_term, False

        my_last_index, my_last_term = self.last_log_index_term()
        log_ok = (
            candidate_last_log_term > my_last_term
            or (
                candidate_last_log_term == my_last_term
                and candidate_last_log_index >= my_last_index
            )
        )
        if not log_ok:
            logger.info(
                "[%s] reject vote for %s: candidate log is stale",
                self.node_id,
                candidate_id,
            )
            return self.current_term, False

        self.voted_for = candidate_id
        self.role = RaftRole.FOLLOWER
        self.last_heartbeat_ts = time.monotonic()

        logger.info(
            "[%s] grant vote to %s in term=%s",
            self.node_id,
            candidate_id,
            self.current_term,
        )
        return self.current_term, True

    # ================== APPENDENTRIES ==================

    def handle_append_entries(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[Dict[str, Any]],
        leader_commit: int,
    ) -> Tuple[int, bool]:
        """
        Обработка RPC AppendEntries (heartbeat + репликация).
        """
        logger.info(
            "[%s] handle_append_entries: from leader=%s term=%s, my_term=%s, prev_idx=%s, prev_term=%s, entries=%d, leader_commit=%s",
            self.node_id,
            leader_id,
            term,
            self.current_term,
            prev_log_index,
            prev_log_term,
            len(entries),
            leader_commit,
        )

        # 1. Устаревший term
        if term < self.current_term:
            logger.info(
                "[%s] reject AppendEntries from %s: stale term %s < %s",
                self.node_id,
                leader_id,
                term,
                self.current_term,
            )
            return self.current_term, False

        # 2. Новый или равный term
        if term > self.current_term:
            self.become_follower(term, leader_id=leader_id)
        else:
            self.role = RaftRole.FOLLOWER
            self.leader_id = leader_id
            self.last_heartbeat_ts = time.monotonic()

        # 3. Проверка prev_log_index/term
        if prev_log_index > self.log.last_index():
            logger.info(
                "[%s] AppendEntries mismatch: prev_log_index %s > last_index %s",
                self.node_id,
                prev_log_index,
                self.log.last_index(),
            )
            return self.current_term, False

        if self.log.term_at(prev_log_index) != prev_log_term:
            logger.info(
                "[%s] AppendEntries term mismatch at index %s: %s != %s, truncating",
                self.node_id,
                prev_log_index,
                self.log.term_at(prev_log_index),
                prev_log_term,
            )
            self.log.truncate_from(prev_log_index + 1)
            return self.current_term, False

        # 4–5. Добавляем новые записи
        next_index = prev_log_index + 1
        for i, command in enumerate(entries):
            entry_index = next_index + i
            existing_entry = self.log.get(entry_index)

            if existing_entry is not None:
                if existing_entry.term != term:
                    logger.info(
                        "[%s] log conflict at index %s: %s != %s, truncating",
                        self.node_id,
                        entry_index,
                        existing_entry.term,
                        term,
                    )
                    self.log.truncate_from(entry_index)
                    new_entries: List[LogEntry] = []
                    for j in range(i, len(entries)):
                        cmd = entries[j]
                        new_entries.append(
                            LogEntry(
                                term=term,
                                index=entry_index + (j - i),
                                command=cmd,
                            )
                        )
                    self.log.append(new_entries)
                    break
            else:
                new_entry = LogEntry(term=term, index=entry_index, command=command)
                self.log.append([new_entry])

        # 6. Обновляем commitIndex
        if leader_commit > self.commit_index:
            new_commit = min(leader_commit, self.log.last_index())
            if new_commit != self.commit_index:
                logger.info(
                    "[%s] commit_index %s -> %s",
                    self.node_id,
                    self.commit_index,
                    new_commit,
                )
            self.commit_index = new_commit

        return self.current_term, True

    # ================== ПРИМЕНЕНИЕ ЗАКОММИЧЕННЫХ ЗАПИСЕЙ ==================

    def apply_committed_entries(self) -> None:
        """
        Применяет все записи из журнала, для которых:
           index <= commitIndex и index > lastApplied.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is None:
                continue
            logger.info(
                "[%s] apply entry index=%s term=%s command=%s",
                self.node_id,
                entry.index,
                entry.term,
                entry.command,
            )
            self.state_machine.apply(entry.command)

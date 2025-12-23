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
#
# Добавлено для пункта (4):
#   - статический состав кластера из NODE_ID + PEERS (env)
#   - защита: отвергаем RPC от узлов вне статического списка

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple
import os

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

    # Адреса остальных узлов (не включая self)
    peer_addresses: Dict[str, str] = field(default_factory=dict)

    state_machine: KeyValueStateMachine = field(default_factory=KeyValueStateMachine)

    # Состояние snapshot на log.base_index (важно: НЕ текущее KV, а именно снапшотное)
    snapshot_state: Optional[Dict[str, Any]] = None

    # Для лидера (RAFT): состояние репликации
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    last_heartbeat_ts: float = field(default_factory=time.monotonic)

    # ================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==================

    def cluster_nodes(self) -> List[str]:
        """Статический список узлов: self + peers из конфигурации."""
        return [self.node_id] + list(self.peers)

    def majority(self) -> int:
        """Кворум: floor(N/2)+1 для статического кластера."""
        return len(self.cluster_nodes()) // 2 + 1

    def last_log_index_term(self) -> Tuple[int, int]:
        """Возвращает (lastLogIndex, lastLogTerm)."""
        return self.log.last_index(), self.log.last_term()

    def to_status_dict(self) -> Dict[str, Any]:
        """Сериализует текущий статус узла для API."""
        return {
            "node_id": self.node_id,
            "role": self.role.name,
            "term": self.current_term,
            "leader_id": self.leader_id,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "last_log_index": self.log.last_index(),
            "last_log_term": self.log.last_term(),
        }

    def validate_invariants(self) -> None:
        """Лёгкие проверки индексных инвариантов (опционально включаемые)."""
        if os.getenv("RAFT_VALIDATE", "0") != "1":
            return
        warnings: List[str] = []
        last_index = self.log.last_index()
        if self.last_applied > self.commit_index:
            warnings.append("last_applied > commit_index")
        if self.commit_index > last_index:
            warnings.append("commit_index > last_log_index")
        if self.commit_index < self.log.base_index:
            warnings.append("commit_index < base_index")
        if warnings:
            logger.warning("[%s][%s][term=%s] invariant warnings: %s", self.node_id, self.role.name, self.current_term, "; ".join(warnings))

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

        # 1) Устаревший term
        if term < self.current_term:
            logger.info("[%s] reject vote for %s: stale term", self.node_id, candidate_id)
            return self.current_term, False

        # 2) Новый term (важно: обновляем term ДО membership-check)
        if term > self.current_term:
            self.become_follower(term)

        # 3) Ограничение по статическому членству: голосуем только за известные узлы
        allowed = set(self.cluster_nodes())
        if candidate_id not in allowed:
            logger.info(
                "[%s] reject vote for %s: not a member of static cluster",
                self.node_id,
                candidate_id,
            )
            return self.current_term, False

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

        Поддерживаем 2 формата entries:
          1) старый: entries=[{op:..., key:..., ...}, ...]  -> term берём из RPC
          2) новый:  entries=[{"term": int, "command": {...}}, ...]
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

        # 0) Сообщения от не-члена текущей конфигурации игнорируем.
        # Это защищает от "устаревшего лидера"/"зомби-лидера" при смене состава.
        # 1) Устаревший term
        if term < self.current_term:
            logger.info(
                "[%s] reject AppendEntries from %s: stale term %s < %s",
                self.node_id,
                leader_id,
                term,
                self.current_term,
            )
            return self.current_term, False

        # 2) Новый term (важно: обновляем term ДО membership-check)
        if term > self.current_term:
            self.become_follower(term, leader_id=leader_id)
        else:
            self.role = RaftRole.FOLLOWER
            self.leader_id = leader_id
            self.last_heartbeat_ts = time.monotonic()

        # 3) Membership-check после term update:
        # если отправитель не member статического кластера — отклоняем, но term уже актуальный
        allowed = set(self.cluster_nodes())
        if leader_id not in allowed:
            logger.info(
                "[%s] reject AppendEntries from %s: not a member of static cluster",
                self.node_id,
                leader_id,
            )
            return self.current_term, False

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

        for i, entry in enumerate(entries):
            entry_index = next_index + i
            existing_entry = self.log.get(entry_index)

            # entry может быть либо командой (старый формат),
            # либо {"term": int, "command": dict} (новый формат)
            if isinstance(entry, dict) and "command" in entry and "term" in entry:
                entry_term = int(entry["term"])
                entry_command = entry["command"]
            else:
                entry_term = term  # fallback: term из RPC
                entry_command = entry

            if existing_entry is not None:
                # ВАЖНО: сравниваем по entry_term, а не по RPC term
                if existing_entry.term != entry_term:
                    logger.info(
                        "[%s] log conflict at index %s: %s != %s, truncating",
                        self.node_id,
                        entry_index,
                        existing_entry.term,
                        entry_term,
                    )
                    # Обрезаем хвост и дописываем все оставшиеся entries
                    self.log.truncate_from(entry_index)

                    new_entries: List[LogEntry] = []
                    for j in range(i, len(entries)):
                        e = entries[j]
                        if isinstance(e, dict) and "command" in e and "term" in e:
                            e_term = int(e["term"])
                            e_cmd = e["command"]
                        else:
                            e_term = term
                            e_cmd = e

                        new_entries.append(
                            LogEntry(
                                term=e_term,
                                index=next_index + j,
                                command=e_cmd,
                            )
                        )

                    if new_entries:
                        self.log.append(new_entries)
                    break
            else:
                new_entry = LogEntry(term=entry_term, index=entry_index, command=entry_command)
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

            # KV-команды
            self.state_machine.apply(entry.command)

        # Лёгкая проверка инвариантов после применения команд
        self.validate_invariants()

    # ================== SNAPSHOT / INSTALLSNAPSHOT ==================

    def maybe_create_snapshot(self, *, threshold: int) -> bool:
        """
        Если с момента последнего snapshot накопилось >= threshold применённых записей,
        делаем snapshot на last_applied и компактим лог.
        """
        if threshold <= 0:
            return False

        # Сколько записей применено поверх base_index
        if (self.last_applied - self.log.base_index) < threshold:
            return False

        last_included_index = self.last_applied
        last_included_term = self.log.term_at(last_included_index)

        # snapshot_state должен соответствовать last_included_index
        self.snapshot_state = self.state_machine.export_state()

        self.log.compact_upto(last_included_index, last_included_term)

        # После снапшота base_index совпадает с last_included_index, так и нужно
        if self.commit_index < self.log.base_index:
            self.commit_index = self.log.base_index
        if self.last_applied < self.log.base_index:
            self.last_applied = self.log.base_index

        logger.info(
            "[%s] snapshot created: last_included_index=%s last_included_term=%s (log compacted)",
            self.node_id,
            self.log.base_index,
            self.log.base_term,
        )
        return True

    def handle_install_snapshot(
        self,
        *,
        term: int,
        leader_id: str,
        last_included_index: int,
        last_included_term: int,
        state: Dict[str, Any],
    ) -> Tuple[int, bool]:
        """
        InstallSnapshot RPC (минимальная версия).
        """
        logger.info(
            "[%s] handle_install_snapshot: from leader=%s term=%s my_term=%s last_included=%s/%s",
            self.node_id,
            leader_id,
            term,
            self.current_term,
            last_included_index,
            last_included_term,
        )

        if term < self.current_term:
            return self.current_term, False

        if term > self.current_term:
            self.become_follower(term, leader_id=leader_id)
        else:
            self.role = RaftRole.FOLLOWER
            self.leader_id = leader_id
            self.last_heartbeat_ts = time.monotonic()

        # Если снапшот уже не новее — игнорируем как успешный
        if last_included_index <= self.log.base_index:
            return self.current_term, True

        # 1) Применяем state machine из snapshot
        self.state_machine.load_state(state)
        self.snapshot_state = dict(state)

        # 2) Компактим лог до last_included_index/term
        # Если в entries есть конфликтующие записи <= last_included_index — они просто исчезнут.
        self.log.compact_upto(last_included_index, last_included_term)

        # 3) Индексы коммита/применения не должны быть "до" snapshot
        if self.commit_index < last_included_index:
            self.commit_index = last_included_index
        if self.last_applied < last_included_index:
            self.last_applied = last_included_index

        self.validate_invariants()
        return self.current_term, True
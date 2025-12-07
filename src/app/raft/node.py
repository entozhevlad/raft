# app/raft/node.py
#
# Основной класс RaftNode: хранит состояние узла и реализует
# ядро алгоритма RAFT (без сети, таймеров и персистентности).
#
# На этом шаге мы делаем:
#   - роли узла (Follower / Candidate / Leader)
#   - структуру состояния (currentTerm, votedFor, log, commitIndex, lastApplied)
#   - обработку RequestVote и AppendEntries как "чистых" методов

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

from src.app.raft.log import LogEntry, RaftLog
from src.app.raft.state_machine import KeyValueStateMachine


class RaftRole(Enum):
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


@dataclass
class RaftNode:
    """
    Реализация одного RAFT-узла (без сетевого слоя).

    node_id  - уникальный идентификатор узла (строка)
    peers    - список идентификаторов других узлов (для будущего сетевого слоя)

    Внутри храним:
      - текущий term
      - за кого голосовали в этом term
      - журнал
      - индексы commitIndex/lastApplied
      - роль узла и опционально leader_id
      - машину состояний KV для применения закоммиченных команд
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

    # Машина состояний key-value
    state_machine: KeyValueStateMachine = field(default_factory=KeyValueStateMachine)

    # Для лидера (инициализируем позже, когда станем лидером):
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    # ================== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==================

    def last_log_index_term(self) -> Tuple[int, int]:
        """
        Удобный метод: возвращает (lastLogIndex, lastLogTerm).
        Используется в логике выборов.
        """
        return self.log.last_index(), self.log.last_term()

    # ================== ПЕРЕХОДЫ МЕЖДУ РОЛЯМИ ==================

    def become_follower(self, new_term: int, leader_id: Optional[str] = None) -> None:
        """
        Переводит узел в состояние Follower.
        Обычно вызывается, когда увидели более новый term.
        """
        self.role = RaftRole.FOLLOWER
        self.current_term = new_term
        self.leader_id = leader_id
        self.voted_for = None  # Сбрасываем голос в новом терме

    def become_candidate(self) -> None:
        """
        Узел начинает выборы: увеличивает term и голосует за себя.
        """
        self.role = RaftRole.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        # Логика отправки RequestVote другим узлам появится позже в сетевом слое.

    def become_leader(self) -> None:
        """
        Узел становится лидером.
        Инициализируем nextIndex и matchIndex для последующей репликации журнала.
        """
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id

        last_index = self.log.last_index()
        self.next_index = {peer_id: last_index + 1 for peer_id in self.peers}
        self.match_index = {peer_id: 0 for peer_id in self.peers}

        # Здесь же в будущем будем запускать периодические heartbeat'ы.

    # ================== ЛОГИКА REQUESTVOTE ==================

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

        Правила (по RAFT):
          1. Если term < currentTerm, голос не отдаём.
          2. Если term > currentTerm, обновляем currentTerm и становимся Follower.
          3. Голос отдаём, если:
             - ещё не голосовали в этом term или уже голосовали за этого кандидата,
             - лог кандидата "не старее", чем наш:
               (его lastLogTerm > наш lastLogTerm) или
               (термы равны и его lastLogIndex >= наш lastLogIndex)
        """
        # 1. Если запрос с устаревшим term — сразу отказываем.
        if term < self.current_term:
            return self.current_term, False

        # 2. Если видим более свежий term — обновляемся и становимся Follower.
        if term > self.current_term:
            self.become_follower(term)

        # Теперь term == current_term.
        # Проверяем, можно ли голосовать за кандидата.
        if self.voted_for is not None and self.voted_for != candidate_id:
            # Уже голосовали за другого кандидата в этом term.
            return self.current_term, False

        # Проверка "свежести" лога кандидата.
        my_last_index, my_last_term = self.last_log_index_term()

        log_ok = (
            candidate_last_log_term > my_last_term
            or (
                candidate_last_log_term == my_last_term
                and candidate_last_log_index >= my_last_index
            )
        )

        if not log_ok:
            return self.current_term, False

        # Все условия соблюдены — отдаём голос.
        self.voted_for = candidate_id
        # Важно: остаёмся Follower (по RAFT).
        self.role = RaftRole.FOLLOWER
        return self.current_term, True

    # ================== ЛОГИКА APPENDENTRIES ==================

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
        Обработка RPC AppendEntries (без сетевого слоя).

        entries — список команд (dict), каждая будет превращена в LogEntry.

        Возвращает (currentTerm, success).

        Правила (по RAFT):
          1. Если term < currentTerm, отвергаем запрос.
          2. Если term >= currentTerm, обновляем currentTerm и становимся Follower.
          3. Проверяем, есть ли у нас запись с index=prevLogIndex и term=prevLogTerm.
             Если нет — возвращаем (currentTerm, False).
          4. Если есть конфликтующие записи (один и тот же индекс, но другой term),
             удаляем текущую запись и все записи после неё.
          5. Добавляем новые записи из entries, которые ещё не в журнале.
          6. Если leaderCommit > commitIndex:
               commitIndex = min(leaderCommit, индекс последней новой записи).
        """
        # 1. Устаревший term — отклоняем.
        if term < self.current_term:
            return self.current_term, False

        # 2. Более свежий term или такой же — обновляемся и становимся Follower.
        if term > self.current_term:
            self.become_follower(term, leader_id=leader_id)
        else:
            # term == current_term, фиксируем лидерство отправителя
            self.role = RaftRole.FOLLOWER
            self.leader_id = leader_id

        # 3. Проверяем согласованность с журналом:
        #    запись с prev_log_index должна иметь term = prev_log_term.
        if prev_log_index > self.log.last_index():
            # У нас журнал короче, чем у лидера в указанной позиции.
            return self.current_term, False

        if self.log.term_at(prev_log_index) != prev_log_term:
            # Конфликт в журнале: удаляем запись с prev_log_index+1 и далее.
            self.log.truncate_from(prev_log_index + 1)
            return self.current_term, False

        # 4-5. Добавляем новые записи, учитывая возможные конфликты по индексам.
        # Индекс первой новой записи.
        next_index = prev_log_index + 1

        for i, command in enumerate(entries):
            entry_index = next_index + i
            existing_entry = self.log.get(entry_index)

            if existing_entry is not None:
                # Если уже есть запись с таким индексом, но другим term — конфликт.
                # Удаляем её и все последующие, затем добавляем оставшиеся.
                if existing_entry.term != term:
                    self.log.truncate_from(entry_index)
                    # После трима текущая и все последующие записи будут перезаписаны.
                    new_entries = []
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
                # Если term совпадает — ничего не делаем, запись уже есть.
            else:
                # Записи в журнале нет — просто добавляем.
                new_entry = LogEntry(
                    term=term,
                    index=entry_index,
                    command=command,
                )
                self.log.append([new_entry])

        # 6. Обновляем commitIndex.
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_index())

        return self.current_term, True

    # ================== ПРИМЕНЕНИЕ ЗАКОММИЧЕННЫХ ЗАПИСЕЙ ==================

    def apply_committed_entries(self) -> None:
        """
        Применяет все записи из журнала, для которых:
           index <= commitIndex и index > lastApplied.
        Каждая команда отправляется в KeyValueStateMachine.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is None:
                # Теоретически не должно случаться, но подстрахуемся.
                continue
            # Команда — это dict, который понимает KeyValueStateMachine.apply(...)
            self.state_machine.apply(entry.command)

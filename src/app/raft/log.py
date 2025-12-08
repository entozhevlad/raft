# src/app/raft/log.py
#
# Журнал RAFT: список LogEntry.
# Умеет:
#   - хранить записи
#   - отдавать last_index/last_term
#   - обрезать хвост
#   - доставать запись по индексу
#
# Логические индексы начинаются с 1.
# "Пустой" журнал имеет last_index = 0, last_term = 0.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]


@dataclass
class RaftLog:
    entries: List[LogEntry] = field(default_factory=list)

    def last_index(self) -> int:
        if not self.entries:
            return 0
        return self.entries[-1].index

    def last_term(self) -> int:
        if not self.entries:
            return 0
        return self.entries[-1].term

    def get(self, index: int) -> Optional[LogEntry]:
        """
        Возвращает запись с данным индексом или None, если её нет.
        """
        if index <= 0:
            return None
        # индексы последовательные, можно обращаться по смещению
        offset = index - 1
        if 0 <= offset < len(self.entries):
            return self.entries[offset]
        return None

    def term_at(self, index: int) -> int:
        """
        Возвращает term записи с данным индексом,
        либо 0, если index == 0 или записи нет.
        """
        if index == 0:
            return 0
        entry = self.get(index)
        if entry is None:
            return 0
        return entry.term

    def append(self, new_entries: List[LogEntry]) -> None:
        """
        Добавляет одну или несколько записей в конец.
        """
        if not new_entries:
            return
        self.entries.extend(new_entries)

    def truncate_from(self, index: int) -> None:
        """
        Обрезает журнал, удаляя записи с индексом >= index.
        """
        if index <= 0:
            self.entries.clear()
            return
        # оставляем все записи с индексом < index
        self.entries = [e for e in self.entries if e.index < index]

    # ==== Сериализация для персистентности ====

    def to_serializable(self) -> List[Dict[str, Any]]:
        """
        Преобразует журнал в список dict-ов для сохранения в JSON.
        """
        return [
            {"term": e.term, "index": e.index, "command": e.command}
            for e in self.entries
        ]

    @classmethod
    def from_serializable(cls, data: List[Dict[str, Any]]) -> "RaftLog":
        entries: List[LogEntry] = []
        for item in data:
            entries.append(
                LogEntry(
                    term=int(item["term"]),
                    index=int(item["index"]),
                    command=item["command"],
                )
            )
        return cls(entries=entries)

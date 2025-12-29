from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LogEntry:
    term: int
    index: int
    command: Dict[str, Any]


@dataclass
class RaftLog:
    """Лог-журнал RAFT."""
    entries: List[LogEntry] = field(default_factory=list)
    base_index: int = 0
    base_term: int = 0

    def last_index(self) -> int:
        if not self.entries:
            return self.base_index
        return self.entries[-1].index

    def last_term(self) -> int:
        if not self.entries:
            return self.base_term
        return self.entries[-1].term

    def get(self, index: int) -> Optional[LogEntry]:
        if index <= self.base_index:
            return None
        offset = index - self.base_index - 1
        if 0 <= offset < len(self.entries):
            return self.entries[offset]
        return None

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.base_index:
            return self.base_term
        if index < self.base_index:
            return 0
        e = self.get(index)
        return 0 if e is None else e.term

    def append(self, new_entries: List[LogEntry]) -> None:
        if not new_entries:
            return
        self.entries.extend(new_entries)

    def truncate_from(self, index: int) -> None:
        """Обрезает хвост, удаляя записи с индексом >= index."""
        if index <= self.base_index + 1:
            self.entries.clear()
            return
        self.entries = [e for e in self.entries if e.index < index]

    def entries_from(self, start_index: int) -> List[LogEntry]:
        """Возвращает срез лога."""
        if start_index <= self.base_index + 1:
            start_index = self.base_index + 1
        offset = start_index - self.base_index - 1
        if offset < 0:
            offset = 0
        if offset >= len(self.entries):
            return []
        return list(self.entries[offset:])

    def compact_upto(self, last_included_index: int, last_included_term: int) -> None:
        """Сжатие лога."""

        if last_included_index <= self.base_index:
            return

        self.entries = [e for e in self.entries if e.index > last_included_index]

        self.base_index = last_included_index
        self.base_term = last_included_term

    def to_serializable(self) -> List[Dict[str, Any]]:
        return [{"term": e.term, "index": e.index, "command": e.command} for e in self.entries]

    @classmethod
    def from_serializable(cls, data: List[Dict[str, Any]], *, base_index: int = 0, base_term: int = 0) -> "RaftLog":
        entries: List[LogEntry] = []
        for item in data:
            entries.append(
                LogEntry(
                    term=int(item["term"]),
                    index=int(item["index"]),
                    command=item["command"],
                )
            )
        return cls(entries=entries, base_index=int(base_index), base_term=int(base_term))
# app/raft/log.py
#
# Реализация журнала (лога) RAFT.
#
# Важно:
#   - Индексы в RAFT начинаются с 1.
#   - Для удобства в self._entries[0] можно хранить "пустую" запись,
#     чтобы индекс записи совпадал с индексом в списке.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class LogEntry:
    """
    Запись в RAFT-журнале.

    term   - номер терма, в котором запись была создана лидером
    index  - позиция записи в журнале (начинается с 1)
    command - произвольная команда для state machine
              в нашем случае это будет операция над KV-хранилищем,
              но алгоритм RAFT не привязан к конкретному типу.
    """
    term: int
    index: int
    command: Any


class RaftLog:
    """
    Обёртка над списком LogEntry, инкапсулирует операции с журналом.
    """

    def __init__(self) -> None:
        # Нулевая запись-заглушка, чтобы индексация начиналась с 1.
        # Её можно считать "виртуальной" записью с term=0, index=0.
        self._entries: List[LogEntry] = [LogEntry(term=0, index=0, command=None)]

    def last_index(self) -> int:
        """Возвращает индекс последней записи в журнале."""
        return self._entries[-1].index

    def last_term(self) -> int:
        """Возвращает term последней записи в журнале."""
        return self._entries[-1].term

    def get(self, index: int) -> Optional[LogEntry]:
        """
        Возвращает запись по индексу или None, если такого индекса нет.
        """
        if index < 0 or index > self.last_index():
            return None
        return self._entries[index]

    def term_at(self, index: int) -> int:
        """
        Возвращает term записи по индексу.
        Если индекс 0 или меньше нуля — считаем, что term=0.
        """
        if index <= 0:
            return 0
        entry = self.get(index)
        return entry.term if entry is not None else 0

    def append(self, entries: List[LogEntry]) -> None:
        """
        Добавляет одну или несколько записей в конец журнала.

        Предполагается, что:
          - индекс каждой новой записи = last_index() + 1, last_index() + 2, ...
        Это ответственность вызывающего кода (лидера при репликации).
        """
        if not entries:
            return
        self._entries.extend(entries)

    def truncate_from(self, index: int) -> None:
        """
        Обрезает журнал, удаляя запись с index и всё, что после неё.
        Используется при разрешении конфликтов AppendEntries.
        """
        if index <= 0:
            # Полностью очищаем журнал до начальной записи-заглушки
            self._entries = [self._entries[0]]
        elif index <= self.last_index():
            # сохраняем записи до index-1 включительно
            self._entries = self._entries[: index]

    def slice_from(self, index: int) -> List[LogEntry]:
        """
        Возвращает срез записей, начиная с index (включительно) до конца.
        Помогает лидеру отправлять "хвост" журнала.
        """
        if index > self.last_index():
            return []
        return self._entries[index:]

    def __len__(self) -> int:
        # Количество реальных записей без нулевой
        return len(self._entries) - 1

    def __iter__(self):
        # Итерируем только по "реальным" записям (без нулевой).
        return iter(self._entries[1:])

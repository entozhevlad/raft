# app/raft/state_machine.py
#
# Простейшая KV-машина состояний, поверх которой работает RAFT.
# Именно сюда применяются закоммиченные команды из журнала.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class KeyValueStateMachine:
    """
    Простое KV-хранилище.
    В реальной жизни здесь могла бы быть отдельная база данных,
    но для учебного проекта достаточно словаря в памяти.
    """
    _storage: Dict[str, Any] = field(default_factory=dict)

    def apply(self, command: Dict[str, Any]) -> Optional[Any]:
        """
        Применяет команду к хранилищу.

        Ожидаемый формат команды:
          - {"op": "put", "key": str, "value": Any}
          - {"op": "delete", "key": str}
        Возвращает:
          - при put: записанное значение
          - при delete: удалённое значение или None, если ключа не было
        """
        op = command.get("op")
        key = command.get("key")

        if op == "put":
            value = command.get("value")
            self._storage[key] = value
            return value

        if op == "delete":
            return self._storage.pop(key, None)

        # На случай неизвестной операции — просто игнорируем.
        # В реальном коде лучше логировать такие ситуации.
        return None

    def get(self, key: str) -> Optional[Any]:
        """Возвращает значение по ключу или None, если нет."""
        return self._storage.get(key)

    def snapshot(self) -> Dict[str, Any]:
        """
        Возвращает "снимок" текущего состояния.
        Для простоты — просто копия словаря.
        Потом мы будем сохранять это на диск.
        """
        return dict(self._storage)

    def load_snapshot(self, data: Dict[str, Any]) -> None:
        """
        Загружает состояние из снапшота.
        Используется при рестарте узла или установке снапшота лидером.
        """
        self._storage = dict(data)

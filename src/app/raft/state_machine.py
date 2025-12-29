from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class KeyValueStateMachine:
    store: Dict[str, Any] = field(default_factory=dict)

    def apply(self, command: Dict[str, Any]) -> None:
        """Применяет одну команду к словарю."""
        op = command.get("op")
        key = command.get("key")

        if op == "put":
            self.store[str(key)] = command.get("value")
        elif op == "delete":
            if key in self.store:
                del self.store[key]
        else:
            pass

    def get(self, key: str) -> Optional[Any]:
        return self.store.get(key)

    def export_state(self) -> Dict[str, Any]:
        """Возвращает текущее состояние KV для сохранения в JSON."""
        return dict(self.store)

    def load_state(self, data: Dict[str, Any]) -> None:
        """Загружает состояние KV из словаря. """
        self.store = dict(data)

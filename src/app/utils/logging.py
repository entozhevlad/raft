# src/app/utils/logging.py
#
# Простая настройка логирования для проекта.
# Делает так, чтобы logger "raft" писал INFO в консоль.

from __future__ import annotations

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """
    Настраивает базовое логирование:
      - корневой логгер
      - логгер "raft"
    """
    # Если уже настроено (handlers есть) — не трогаем
    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Для надёжности явно выставим уровень для нашего логгера
    raft_logger = logging.getLogger("raft")
    raft_logger.setLevel(level)

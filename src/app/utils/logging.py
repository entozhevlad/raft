from __future__ import annotations

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """Настраивает базовое логирование."""

    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    raft_logger = logging.getLogger("raft")
    raft_logger.setLevel(level)

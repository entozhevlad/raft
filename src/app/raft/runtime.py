from __future__ import annotations

import logging
from typing import List

from fastapi import FastAPI

from src.app.raft.node import RaftNode
from src.app.raft.timers import start_background_tasks, stop_background_tasks

logger = logging.getLogger("raft")


def setup_runtime(app: FastAPI) -> None:
    """Регистрация старта/остановки фоновых задач RAFT."""

    @app.on_event("startup")
    async def _start() -> None:
        node: RaftNode = app.state.raft_node
        logger.info("[%s] start RAFT runtime", node.node_id)
        app.state._raft_tasks = start_background_tasks(app, node)

    @app.on_event("shutdown")
    async def _stop() -> None:
        tasks: List = getattr(app.state, "_raft_tasks", [])
        if tasks:
            logger.info("Stopping RAFT runtime tasks")
            await stop_background_tasks(tasks)


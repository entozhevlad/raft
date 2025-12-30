from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, List

from fastapi import FastAPI

from src.app.raft.node import RaftNode
from src.app.raft.timers import start_background_tasks, stop_background_tasks

logger = logging.getLogger("raft")


@asynccontextmanager
async def raft_lifespan(app: FastAPI) -> AsyncIterator[None]:
    node: RaftNode = app.state.raft_node
    logger.info("[%s] start RAFT runtime", node.node_id)

    tasks = start_background_tasks(app, node)
    app.state._raft_tasks = tasks
    try:
        yield
    finally:
        if tasks:
            logger.info("[%s] stopping RAFT runtime tasks", node.node_id)
            await stop_background_tasks(tasks)


def setup_runtime(app: FastAPI) -> None:
    """Регистрация lifespan-хендлера для RAFT."""
    app.router.lifespan_context = raft_lifespan
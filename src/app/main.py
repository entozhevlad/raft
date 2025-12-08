from fastapi import FastAPI

from src.app.api.healthz import router as health_router
from src.app.api.raft_routes import router as raft_router
from src.app.api.kv_routes import router as kv_router
from src.app.config import load_node_config
from src.app.raft.node import RaftNode
from src.app.raft.timers import setup_raft_background_tasks
from src.app.utils.logging import setup_logging


def create_app() -> FastAPI:
    # === Логирование ===
    setup_logging()

    app = FastAPI(
        title="Raft KV Store",
        version="0.1.0",
        description="Учебное распределённое key-value хранилище на основе RAFT",
    )

    # === Конфигурация узла ===
    config = load_node_config()

    # === Инициализация RAFT-узла ===
    raft_node = RaftNode(node_id=config.node_id, peers=config.peers)

    # Кладём узел и адреса пиров в state приложения.
    app.state.raft_node = raft_node
    app.state.peer_addresses = config.peer_addresses

    # === Фоновые задачи RAFT (выборы + heartbeat) ===
    setup_raft_background_tasks(app)

    # === Подключаем роутеры ===
    app.include_router(health_router)
    app.include_router(raft_router, prefix="/raft")
    app.include_router(kv_router, prefix="/kv")

    return app


app = create_app()

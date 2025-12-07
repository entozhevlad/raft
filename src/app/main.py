# app/main.py
#
# Точка входа FastAPI-приложения.
# Здесь мы:
#   - создаём FastAPI-приложение
#   - инициализируем RaftNode
#   - подключаем роуты (health + raft)

from fastapi import FastAPI

from src.app.api.healthz import router as health_router
from src.app.api.raft_routes import router as raft_router
from src.app.config import load_node_config
from src.app.raft.node import RaftNode


def create_app() -> FastAPI:
    """
    Фабрика приложения.
    В будущем здесь будем:
      - добавлять таймеры RAFT (election timeout, heartbeat)
      - подключать KV-роуты и т.д.
    """
    app = FastAPI(
        title="Raft KV Store",
        version="0.1.0",
        description="Учебное распределённое key-value хранилище на основе RAFT",
    )

    # === Инициализация RAFT-узла ===
    config = load_node_config()
    raft_node = RaftNode(node_id=config.node_id, peers=config.peers)

    # Кладём узел в state приложения.
    # Через это поле мы будем получать доступ к RAFT-логике в роутерах.
    app.state.raft_node = raft_node

    # === Подключаем роутеры ===
    app.include_router(health_router)
    app.include_router(raft_router, prefix="/raft")

    return app


# Этот объект будет использовать uvicorn: app.main:app
app = create_app()

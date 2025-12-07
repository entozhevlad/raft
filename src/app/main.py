# app/main.py
#
# Точка входа FastAPI-приложения.
# Здесь мы создаём объект приложения и подключаем роуты.

from fastapi import FastAPI

from src.app.api.healthz import router as health_router


def create_app() -> FastAPI:
    """
    Фабрика приложения.
    В будущем здесь будем инициализировать:
      - объект RaftNode
      - конфиг узла
      - логирование
      - фоновые задачи
    """
    app = FastAPI(
        title="Raft KV Store",
        version="0.1.0",
        description="Учебное распределённое key-value хранилище на основе RAFT",
    )

    app.include_router(health_router)

    return app


# Этот объект будет использовать uvicorn: app.main:app
app = create_app()

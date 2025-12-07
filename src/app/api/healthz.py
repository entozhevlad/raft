from fastapi import APIRouter

router = APIRouter(tags=["health"])


async def health():
    """
    Простейшая проверка работоспособности сервиса.
    """
    return {"status": "ok"}


async def info():
    """
    Базовая информация о сервисе.
    В будущем сюда можно добавить идентификатор узла и его роль в RAFT.
    """
    return {
        "service": "raft-kv-store",
        "version": "0.1.0",
        "message": "Base FastAPI app is running",
    }


# --- РЕГИСТРАЦИЯ МАРШРУТОВ ---
router.add_api_route(
    path="/health",
    endpoint=health,
    methods=["GET"],
    summary="Health check",
)

router.add_api_route(
    path="/info",
    endpoint=info,
    methods=["GET"],
    summary="Base service information",
)

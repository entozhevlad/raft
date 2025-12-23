from fastapi import APIRouter

router = APIRouter(tags=["health"])


async def health():
    return {"status": "ok"}


router.add_api_route(
    path="/health",
    endpoint=health,
    methods=["GET"],
    summary="Health check",
)

from fastapi import APIRouter
from fractal_server.config import Settings
from fractal_server.config import get_settings

router_default = APIRouter()


@router_default.get("/endpoint/")
async def endpoint() -> Settings:
    settings = get_settings()
    return settings

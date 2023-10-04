"""
`api` module
"""
from fastapi import APIRouter

from ...get_settings import get_settings
from .v1.project import router as project_router

router_default = APIRouter()
router_v1 = APIRouter()

router_v1.include_router(project_router, prefix="/project", tags=["Projects"])


@router_default.get("/alive/")
async def alive():
    settings = get_settings()
    return dict(
        alive=True,
        version=settings.PROJECT_VERSION,
    )

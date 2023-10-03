"""
`api` module
"""
from fastapi import APIRouter

from ...config import get_settings
from ...syringe import Inject
from .v1.project import router as project_router

router_default = APIRouter()
router_v1 = APIRouter()

router_v1.include_router(project_router, prefix="/project", tags=["Projects"])

@router_default.get("/alive/")
async def alive():
    settings = Inject(get_settings)
    return dict(
        alive=True,
        version=settings.PROJECT_VERSION,
    )

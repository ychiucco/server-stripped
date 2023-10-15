import pytest
import asyncio
from fractal_server.config import Settings
from fractal_server.main import router

from typing import Any
from typing import AsyncGenerator
from fastapi import FastAPI
from httpx import AsyncClient


MODULES_TO_PATCH = ["fractal_server.config", "fractal_server.app.api"]

def default_test_settings() -> Settings:
    return Settings(X="fancy y",Y=24)

@pytest.fixture(scope="session")
def event_loop():
    _event_loop = asyncio.new_event_loop()
    _event_loop.set_debug(True)

    yield _event_loop

@pytest.fixture(scope="session", autouse=True)
async def set_default_test_settings():

    with pytest.MonkeyPatch.context() as mp:
        for module in MODULES_TO_PATCH:
            mp.setattr(
                f"{module}.get_settings",
                lambda: default_test_settings(),
                raising=False,
            )
        yield


@pytest.fixture
async def override_settings_startup(monkeypatch, request):

    settings = default_test_settings()
    try:
        for k, v in request.param.items():
            setattr(settings, k, v)
    except AttributeError:
        return  # `request` has no `param`
    Settings(**settings.dict())  # run pydantic type checking
    
    
    for module in MODULES_TO_PATCH:
        monkeypatch.setattr(
            f"{module}.get_settings",
            lambda: settings,
            raising=False,
        )



@pytest.fixture
async def override_settings_runtime(monkeypatch, override_settings_startup):
    
    def _override_settings_runtime(**kwargs):
        from fractal_server.config import get_settings

        settings = get_settings()
        for k, v in kwargs.items():
            setattr(settings, k, v)
        Settings(**settings.dict())  # run pydantic type checking

        for module in MODULES_TO_PATCH:
            monkeypatch.setattr(
                f"{module}.get_settings", lambda: settings, raising=False
            )
        
    return _override_settings_runtime



@pytest.fixture
async def app(override_settings_startup) -> AsyncGenerator[FastAPI, Any]:
    app = FastAPI()
    yield app


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient, Any]:
    app.include_router(router, prefix="/api")
    from fastapi.testclient import TestClient
    yield TestClient(app)
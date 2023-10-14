import contextlib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app.api import router_default
from .app.db import get_db

from .get_settings import get_settings

settings = get_settings()


def start_application() -> FastAPI:

    app = FastAPI()
    app.include_router(router_default, prefix="/api")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "set-cookie",
            "Set-Cookie",
            "Content-Type",
            "Access-Control-Allow-Headers",
            "X-Requested-With",
        ],
        allow_credentials=True,
    )

    return app

app = start_application()

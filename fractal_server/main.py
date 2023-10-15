from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .app.api import router

def start_application() -> FastAPI:

    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app

app = start_application()

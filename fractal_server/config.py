from pydantic import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    X: str = "default x"
    Y: int = 42

@lru_cache
def get_settings(settings = Settings()):
    return settings
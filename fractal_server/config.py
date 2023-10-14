from pydantic import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    name: str
    age: int

@lru_cache
def get_settings(settings = Settings()):
    return settings
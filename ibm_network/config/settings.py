from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IBM_NET_", env_file=".env", extra="ignore")

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "devpassword"
    neo4j_database: str = "neo4j"

    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5-coder:14b"
    llm_timeout_seconds: float = 120.0

    output_dir: str = "out"
    enable_llm_polish: bool = Field(default=True)


def get_settings() -> Settings:
    return Settings()

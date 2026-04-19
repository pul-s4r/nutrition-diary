from __future__ import annotations

from datetime import time
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ND_", env_file=".env", extra="ignore")

    data_dir: Path = Field(default=Path("data"))
    db_path: Path = Field(default=Path("data/nutrition.db"))
    exports_dir: Path = Field(default=Path("data/exports"))
    stages_dump_dir: Path = Field(default=Path("data/stages"))

    # Meal windows (local time)
    breakfast_start: time = Field(default=time(5, 0))
    breakfast_end: time = Field(default=time(10, 30))
    lunch_start: time = Field(default=time(11, 0))
    lunch_end: time = Field(default=time(14, 30))
    dinner_start: time = Field(default=time(17, 0))
    dinner_end: time = Field(default=time(21, 0))

    # Recognition
    recognizer: str = Field(default="mock")  # "mock" or "bedrock"
    bedrock_region: str = Field(default="us-east-1")
    bedrock_model_id: str = Field(default="us.anthropic.claude-sonnet-4-6-20260310-v1:0")
    max_concurrent_requests: int = Field(default=5)
    max_spend_per_run: float = Field(default=5.00)

    # Grounding (USDA)
    usda_api_key: str | None = Field(default=None)
    usda_cache_ttl_days: int = Field(default=90)
    usda_negative_cache_ttl_days: int = Field(default=7)
    usda_match_threshold: float = Field(default=0.65)

    # Google Drive (optional `pip install nutrition-diary[gdrive]`)
    gdrive_credentials_path: Path | None = Field(default=None)
    gdrive_token_path: Path = Field(default=Path("data/gdrive_token.json"))
    gdrive_scopes: list[str] = Field(
        default_factory=lambda: ["https://www.googleapis.com/auth/drive.readonly"],
    )
    gdrive_staging_dir: Path = Field(default=Path("data/gdrive_staging"))


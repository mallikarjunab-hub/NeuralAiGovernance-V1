from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    APP_NAME:    str  = "Neural AI Governance – DSSS"
    APP_VERSION: str  = "3.0.0"
    ENVIRONMENT: str  = "development"
    DEBUG:       bool = False
    LOG_LEVEL:   str  = "INFO"

    # ── Neon PostgreSQL (data queries + RAG) ──────────────────
    NEON_DATABASE_URL:  str = ""          # postgresql://...neon.tech/...
    MAX_SQL_ROWS:       int = 1000
    NEON_POOL_SIZE:     int = 2
    NEON_MAX_OVERFLOW:  int = 2

    # ── Gemini AI ──────────────────────────────────────────────
    GEMINI_API_KEY: str = ""   

    # ── App ────────────────────────────────────────────────────
    SECRET_KEY:   str = ""
    CORS_ORIGINS: str = "http://localhost:8000,http://localhost:3000"

    REDIS_URL:         str = "redis://localhost:6379"
    CACHE_TTL_SECONDS: int = 300

    class Config:
        env_file       = ".env"
        case_sensitive = True
        extra          = "allow"

    @property
    def cors_origins_list(self) -> List[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",")]

settings = Settings()

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    postgres_db: str = "tickeragent"
    postgres_user: str = "tickeragent"
    postgres_password: str = "tickeragent"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    
    redis_host: str = "localhost"
    redis_port: int = 6379
    
    ollama_host: str = "http://localhost:11434"
    

    
    ingest_min_interval_seconds: int = 60
    ingest_user_agent: str = "TickerAgentBot/0.1 (+https://localhost)"
    ingest_allowed_domains: str = ""
    ingest_policy_require_robots: bool = True
    ingest_policy_deny_on_robots_error: bool = False
    
    ollama_embed_model: str = "nomic-embed-text"
    ollama_generate_model: str = "qwen2.5:1.5b"
    embedding_vector_size: int = 768
    
    api_write_key: str = "tickeragent-dev-key"
    api_cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    
    retention_insights_days: int = 90
    retention_snapshots_days: int = 90
    retention_reports_days: int = 180
    retention_search_days: int = 60
    retention_audit_days: int = 365

    # --- Data Provider API Keys ---
    sec_edgar_user_agent: str = "TickerAgent admin@localhost"
    fred_api_key: str = ""
    alpha_vantage_api_key: str = ""
    fmp_api_key: str = ""

    # --- Pipeline settings ---
    intelligence_pipeline_timeout: int = 600

    # --- LLM performance tuning ---
    llm_cache_ttl_seconds: int = 900
    ollama_max_concurrent: int = 2
    ollama_request_timeout: float = 120.0

    # --- Cloud LLM provider (primary) ---
    llm_provider: str = "gemini"
    llm_api_key: str = ""
    gemini_api_key: str = ""
    llm_api_base_url: str = ""
    llm_cloud_model: str = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.llm_api_key and self.gemini_api_key:
            self.llm_api_key = self.gemini_api_key

    @property
    def postgres_dsn(self) -> str:
        return (
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password} host={self.postgres_host} "
            f"port={self.postgres_port}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    @property
    def cors_origins(self) -> list[str]:
        origins = [origin.strip() for origin in self.api_cors_origins.split(",")]
        return [origin for origin in origins if origin]

@lru_cache
def get_settings() -> Settings:
    return Settings()

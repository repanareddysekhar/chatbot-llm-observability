from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://obs:obs@localhost:5432/obs"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"
    ingest_api_key: str = "dev-key"
    environment: str = "dev"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

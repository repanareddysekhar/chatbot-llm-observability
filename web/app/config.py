from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://obs:obs@localhost:5432/obs"
    ingest_url: str = "http://localhost:4000"
    ingest_api_key: str = "dev-key"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    google_api_key: str = ""
    environment: str = "dev"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

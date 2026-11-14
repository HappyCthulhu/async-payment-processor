from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    rabbitmq_url: str
    rabbitmq_management_url: str
    api_key: str
    db_echo: bool = False

    relay_batch_size: int = 10
    relay_poll_interval_seconds: float = 1.0
    relay_shutdown_timeout_seconds: float = 10.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="APP_",
        extra="ignore",
    )


settings = Settings()

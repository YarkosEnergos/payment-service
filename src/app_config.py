from typing import Self

from pydantic import BaseModel, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class BaseConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env", ".env.local"), extra="ignore")


class ApiConfig(BaseConfig):
    api_key: SecretStr = Field(..., min_length=1)


class PostgresConfig(BaseConfig):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")

    host: str
    port: int = 5432
    user: str
    password: SecretStr
    db: str

    @property
    def url(self) -> URL:
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.user,
            password=self.password.get_secret_value(),
            host=self.host,
            port=self.port,
            database=self.db,
        )


class RabbitMQConfig(BaseConfig):
    model_config = SettingsConfigDict(env_prefix="RABBITMQ_")

    url: SecretStr


class GatewayConfig(BaseConfig):
    model_config = SettingsConfigDict(env_prefix="GATEWAY_")

    min_delay_seconds: float = Field(default=2.0, ge=0)
    max_delay_seconds: float = Field(default=5.0, ge=0)
    success_rate: float = Field(default=0.9, ge=0, le=1)

    @model_validator(mode="after")
    def check_delay_range(self) -> Self:
        if self.min_delay_seconds > self.max_delay_seconds:
            msg = "GATEWAY_MIN_DELAY_SECONDS must not exceed GATEWAY_MAX_DELAY_SECONDS"
            raise ValueError(msg)
        return self


class ConsumerConfig(BaseConfig):
    model_config = SettingsConfigDict(env_prefix="CONSUMER_")

    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    prefetch_count: int = Field(default=10, ge=1)
    webhook_timeout_seconds: float = Field(default=10.0, gt=0)


class OutboxConfig(BaseConfig):
    model_config = SettingsConfigDict(env_prefix="OUTBOX_")

    poll_interval_seconds: float = Field(default=1.0, gt=0)
    batch_size: int = Field(default=100, ge=1)


class AppConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    rabbitmq: RabbitMQConfig = Field(default_factory=RabbitMQConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    consumer: ConsumerConfig = Field(default_factory=ConsumerConfig)
    outbox: OutboxConfig = Field(default_factory=OutboxConfig)


app_config = AppConfig()

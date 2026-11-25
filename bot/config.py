from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    telegram_bot_token: SecretStr | None = None
    database_url: str = "postgresql+asyncpg://bot@db/bot"
    scan_seconds: int = Field(1, ge=1)
    candidates: int = Field(48, ge=1, le=200)
    rest_requests_per_second: float = Field(5, ge=1, le=10)
    quote_age: int = Field(30, ge=1)
    max_spread_bps: float = Field(25, gt=0)
    window: int = Field(20, ge=5, le=48)
    volume_ratio: float = Field(1.4, gt=0)
    breakout_min_atr: float = Field(0, ge=0)
    breakout_close_fraction: float = Field(0, ge=0, le=1)
    rsi_low: float = Field(50, ge=0, le=100)
    rsi_high: float = Field(78, ge=0, le=100)
    atr_min: float = Field(0.001, gt=0)
    atr_max: float = Field(0.05, gt=0)
    stop_atr: float = Field(0.6, gt=0)
    min_rr: float = Field(2, gt=0)
    cooldown: int = Field(1800, gt=0)
    expiry: int = Field(900, gt=0)
    balance: float = Field(50, gt=0)
    risk: float = Field(0.01, gt=0, le=1)
    leverage: float = Field(3, ge=1, le=5)
    daily_loss: float = Field(0.03, gt=0, le=1)
    fee: float = Field(0.0005, ge=0, lt=0.1)
    slip: float = Field(0.0003, ge=0, lt=0.1)


cfg = Settings()

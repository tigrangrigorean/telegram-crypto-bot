import os
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column as col

from .config import cfg


def now():
    return datetime.now(UTC)


def stamp():
    return col(DateTime(timezone=True), default=now, nullable=False)


class Base(DeclarativeBase):
    id = col(Integer, primary_key=True)


class User(Base):
    __tablename__ = "users"
    chat_id = col(BigInteger, unique=True, nullable=False)
    notifications_enabled = col(Boolean, nullable=False)
    created_at = stamp()


class Instrument(Base):
    __tablename__ = "instruments"
    symbol = col(String, unique=True, nullable=False)
    status = col(String, nullable=False)
    details = col(JSON, nullable=False)
    updated_at = stamp()


class Signal(Base):
    __tablename__ = "signals"
    __table_args__ = (UniqueConstraint("symbol", "bar"),)
    symbol = col(String, index=True, nullable=False)
    bar = col(BigInteger, nullable=False)
    direction = col(String, nullable=False)
    strategy = col(String, default="breakout")
    entry_price = col(Float, nullable=False)
    stop_loss = col(Float, nullable=False)
    take_profit_1 = col(Float, nullable=False)
    take_profit_2 = col(Float, nullable=False)
    score = col(Integer, nullable=False)
    details = col(JSON, nullable=False)
    status = col(String, index=True, nullable=False)
    created_at = stamp()
    updated_at = stamp()


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("signal_id", "event_type"),)
    signal_id = col(ForeignKey("signals.id"), nullable=False)
    event_type = col(String, nullable=False)
    event_price = col(Float, nullable=False)
    created_at = stamp()


class Delivery(Base):
    __tablename__ = "deliveries"
    __table_args__ = (UniqueConstraint("event_id", "user_id"),)
    event_id = col(ForeignKey("events.id"), nullable=False)
    user_id = col(ForeignKey("users.id"), nullable=False)
    sent = col(Boolean, default=False, index=True)


class Account(Base):
    __tablename__ = "accounts"
    initial_balance = col(Float, nullable=False)
    current_balance = col(Float, nullable=False)
    created_at = stamp()


class Position(Base):
    __tablename__ = "positions"
    signal_id = col(ForeignKey("signals.id"), unique=True, nullable=False)
    symbol = col(String)
    direction = col(String)
    entry_price = col(Float)
    quantity = col(Float)
    leverage = col(Float)
    stop_loss = col(Float)
    take_profit = col(Float)
    status = col(String, index=True)
    opened_at = stamp()
    closed_at = col(DateTime(timezone=True))


class Trade(Base):
    __tablename__ = "trades"
    position_id = col(ForeignKey("positions.id"), unique=True, nullable=False)
    entry_price = col(Float)
    exit_price = col(Float)
    pnl = col(Float)
    fees = col(Float)
    closed_at = stamp()


class Run(Base):
    __tablename__ = "backtest_runs"
    symbol = col(String)
    strategy = col(String)
    timeframe = col(String)
    total_trades = col(Integer)
    win_rate = col(Float)
    net_pnl = col(Float)
    max_drawdown = col(Float)
    metrics = col(JSON)
    created_at = stamp()


class HistoricalTrade(Base):
    __tablename__ = "backtest_trades"
    backtest_run_id = col(ForeignKey("backtest_runs.id"), index=True)
    direction = col(String)
    entry_price = col(Float)
    exit_price = col(Float)
    pnl = col(Float)
    opened_at = stamp()
    closed_at = stamp()


url = make_url(cfg.database_url)
if password_file := os.environ.get("DB_PASSWORD_FILE"):
    url = url.set(password=Path(password_file).read_text().strip())
engine = create_async_engine(url, pool_pre_ping=True)
sessions = async_sessionmaker(engine, expire_on_commit=False)

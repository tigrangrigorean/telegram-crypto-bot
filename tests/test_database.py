"""Run against a migrated PostgreSQL database with TEST_POSTGRES=1."""

import os

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from bot.db import (
    Account,
    Delivery,
    Event,
    Position,
    Signal,
    Trade,
    User,
    engine,
    sessions,
)
from bot.paper import settle
from bot.service import event


async def exercise():
    async with sessions() as db:
        async with db.begin():
            account = await db.get(Account, 1)
            if not account:
                account = Account(id=1, current_balance=50, initial_balance=50)
                db.add(account)
            before = account.current_balance
            db.add(User(chat_id=-987654321987, notifications_enabled=True))
            s = Signal(
                symbol="TEST-USDT",
                bar=111,
                direction="LONG",
                entry_price=100,
                stop_loss=98,
                take_profit_1=106,
                take_profit_2=110,
                score=81,
                status="WATCH",
                details={"rr": 3},
            )
            db.add(s)
            await db.flush()
            for state, price in [
                ("WAITING", 99),
                ("ENTRY CONFIRMED", 100),
                ("TP HIT", 106),
            ]:
                await event(db, s, state, price)
                await settle(db, s, state, price, {"quantityPrecision": 3})
            await db.flush()
            assert account.current_balance > before
            pos = await db.scalar(select(Position).where(Position.signal_id == s.id))
            trade = await db.scalar(select(Trade).where(Trade.position_id == pos.id))
            assert trade.pnl == pytest.approx(account.current_balance - before)
            assert (
                await db.scalar(
                    select(func.count())
                    .select_from(Delivery)
                    .join(Event)
                    .where(Event.signal_id == s.id)
                )
                >= 3
            )
            with pytest.raises(IntegrityError):
                async with db.begin_nested():
                    await event(db, s, "TP HIT", 106)
            await db.rollback()
    await engine.dispose()


@pytest.mark.skipif(
    os.getenv("TEST_POSTGRES") != "1", reason="requires migrated test database"
)
@pytest.mark.asyncio
async def test_postgres_signal_to_trade():
    await exercise()


@pytest.mark.skipif(os.getenv("TEST_POSTGRES")!="1",reason="requires migrated test database")
@pytest.mark.asyncio
async def test_application_boot(monkeypatch):
    import asyncio
    from pathlib import Path
    from unittest.mock import AsyncMock
    from pydantic import SecretStr
    from bot import main
    from bot.market import PublicAPI

    async def local_polling(self,bot,**kwargs):
        for _ in range(100):
            if Path("/tmp/bot-heartbeat").exists():
                break
            await asyncio.sleep(.02)
        assert Path("/tmp/bot-heartbeat").exists()
        assert await bot.get_me()=="mock identity"

    monkeypatch.setattr(main.cfg,"telegram_bot_token",SecretStr("123456:local_fixture"))
    monkeypatch.setattr(main.Bot,"get_me",AsyncMock(return_value="mock identity"))
    monkeypatch.setattr(PublicAPI,"instruments",AsyncMock(return_value={"TEST-USDT":{"pricePrecision":2}}))
    monkeypatch.setattr(PublicAPI,"tickers",AsyncMock(return_value=[]))
    from aiogram import Dispatcher
    monkeypatch.setattr(Dispatcher,"start_polling",local_polling)
    await main.main()

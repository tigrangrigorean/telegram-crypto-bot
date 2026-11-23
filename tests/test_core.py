import asyncio
import gzip
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import httpx
import numpy as np
import pandas as pd
import pytest

from bot import backtest, market, telegram
from bot.config import cfg
from bot.market import MS, PublicAPI, parse, rank
from bot.paper import result, sizing
from bot.strategy import indicators, setup, transition

END = 1800000000000


def frames(side=1):
    frames = {}
    for tf, step in MS.items():
        price = 100 + side * np.arange(60) * 0.01
        f = indicators(
            pd.DataFrame(
                dict(
                    time=END - np.arange(60, 0, -1) * step,
                    open=price,
                    high=price + 1,
                    low=price - 1,
                    close=price,
                    volume=np.ones(60),
                )
            )
        )
        f.loc[
            59,
            [
                "close",
                "high",
                "low",
                "ema9",
                "ema21",
                "ema50",
                "hist",
                "rsi",
                "rv",
                "atr",
                "res",
                "sup",
            ],
        ] = [
            100 + side * 0.2,
            100.3,
            99.7,
            100 + side,
            100,
            100 - side,
            side,
            60 if side == 1 else 40,
            2,
            0.5,
            100 if side == 1 else 106,
            94 if side == 1 else 100,
        ]
        frames[tf] = f
    return frames


@pytest.mark.parametrize("side", [1, -1])
def test_breakout_and_lifecycle(side):
    ff = frames(side)
    idea = setup(ff, 2, END)
    assert idea["direction"] == ("LONG" if side == 1 else "SHORT")
    assert idea["details"]["rr"] >= 2 and 0 <= idea["score"] <= 100
    s = SimpleNamespace(**idea, status="WAITING")
    assert transition(s, s.entry_price, 0) == "ENTRY CONFIRMED"
    assert transition(s, s.stop_loss, 0) == "CANCELLED"
    assert transition(s, s.entry_price, cfg.expiry) == "EXPIRED"
    s.status = "ENTRY CONFIRMED"
    assert transition(s, s.stop_loss, 0) == "SL HIT"
    assert transition(s, s.take_profit_1, 0) == "TP HIT"
    assert setup(ff, 2, END + 300000) is None
    ff["5m"].loc[59, "rv"] = 0
    assert setup(ff, 2, END) is None


def test_indicators_prior_levels():
    f = frames()["5m"][["time", "open", "high", "low", "close", "volume"]]
    r = indicators(f).iloc[-1]
    assert r.res == f.high.iloc[-21:-1].max()
    assert r.rv == 1 and r.atr > 0


@pytest.mark.asyncio
async def test_public_client():
    rows = [
        dict(symbol="X-USDT", currency="USDT", status=1),
        dict(symbol="Y-USDT", currency="USDT", status=0),
        dict(symbol="Z-USD", currency="USD", status=1),
    ]

    def serve(req):
        assert req.method == "GET" and req.url.path.endswith("/v2/quote/contracts")
        return httpx.Response(200, json={"code": 0, "data": rows})

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
        assert list(await PublicAPI(client).instruments()) == ["X-USDT"]


def test_rank_and_parse():
    row = dict(
        symbol="X",
        quoteVolume=1200,
        lastPrice=2,
        highPrice=3,
        lowPrice=1,
        priceChangePercent=5,
    )
    assert rank([row], {"X": {}}, {}) == ["X"]
    data = {"dataType": "X@trade", "data": [{"s": "X", "p": "12.3", "T": 123}]}
    assert parse(gzip.compress(json.dumps(data).encode())) == [("X", 12.3, 123)]
    assert parse("Ping") == "Pong"


@pytest.mark.asyncio
async def test_stream_reconnect(monkeypatch):
    session = MagicMock()
    count = 0

    def connect(*args, **kwargs):
        nonlocal count
        count += 1
        if count == 1:
            raise aiohttp.ClientConnectionError()
        raise asyncio.CancelledError()

    session.ws_connect.side_effect = connect
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    monkeypatch.setattr(market.aiohttp, "ClientSession", lambda **kw: ctx)
    monkeypatch.setattr(market.asyncio, "sleep", AsyncMock())
    with pytest.raises(asyncio.CancelledError):
        await market.stream(lambda: ("X",), lambda *args: None)
    assert count == 2


def test_accounting():
    e, q = sizing(50, 50, 100, 98, 1)
    x, fees, p = result(e, 98, q, 1, 3)
    assert fees > 0 and x < 98 and p == pytest.approx(-0.5)
    assert e * q / 3 <= 50 and sizing(50, 0, 100, 98, 1)[1] == 0
    assert result(e, 0, q, 1, 3)[2] >= -e * q / 3


def test_backtest_causality(monkeypatch):
    ff = {
        tf: f[["time", "open", "high", "low", "close", "volume"]].copy()
        for tf, f in frames().items()
    }
    seen = []

    def decide(view, precision, end):
        for tf, f in view.items():
            assert (f.time + MS[tf] <= end).all()
        seen.append(end)
        if len(seen) == 1:
            return dict(
                direction="LONG", entry_price=100, stop_loss=99.5, take_profit_1=103
            )

    monkeypatch.setattr(backtest, "setup", decide)
    # Next bar opens at the trigger and touches both stop and target.
    ff["5m"].loc[1, ["open", "high", "low"]] = [100, 105, 98]
    trades = backtest.simulate(ff, 2, ff["5m"].time.min(), END)
    assert len(trades) == 1 and trades[0]["pnl"] < 0
    assert backtest.metrics(trades)["max_drawdown"] > 0


@pytest.mark.asyncio
async def test_notification_delivery(monkeypatch):
    idea = setup(frames(), 2, END)
    s = SimpleNamespace(**idea)
    delivery = SimpleNamespace(sent=False)
    event = SimpleNamespace(
        event_type="WAITING", event_price=100, created_at=datetime.now(UTC)
    )
    db = AsyncMock()
    result = MagicMock()
    result.first.return_value = (delivery, SimpleNamespace(chat_id=123), event, s)
    db.execute.return_value = result
    factory = MagicMock()
    factory.begin.return_value.__aenter__.return_value = db
    monkeypatch.setattr(telegram, "sessions", factory)
    bot = AsyncMock()
    await telegram.notify(bot)
    assert delivery.sent and "Ждём входа" in bot.send_message.await_args.args[1]


def test_market_rotation_while_priority_is_busy():
    from bot.service import Service

    svc = Service(None)
    svc.instruments = {str(i): {} for i in range(888)}
    svc.top = list(svc.instruments)[:48]
    selected = set()
    for slot in range(12):
        for _ in range(8):
            batch = svc.batch(slot)
            assert len(batch) <= 16
            assert all(svc.last_attempt.get(s) != slot for s in batch)
            selected.update(batch)
            svc.last_attempt.update(dict.fromkeys(batch, slot))
    assert selected == set(svc.instruments)


@pytest.mark.asyncio
async def test_fast_rejection_avoids_slow_timeframes():
    from bot.service import Service

    api = AsyncMock()
    api.bars.return_value = pd.DataFrame(
        columns=["time", "open", "high", "low", "close", "volume"], dtype=float
    )
    assert await Service(api).analyze("TEST-USDT") is None
    api.bars.assert_awaited_once_with("TEST-USDT", "5m")


@pytest.mark.parametrize("d", [1, -1])
def test_costs_reject_marginal_breakouts(d):
    from bot.strategy import net_rr

    stop, target = 100 - d, 100 + 2 * d
    assert 0 < net_rr(100, stop, target, d, cfg) < 2
    signal = SimpleNamespace(
        direction="LONG" if d == 1 else "SHORT",
        status="WAITING",
        stop_loss=stop,
        entry_price=100,
        take_profit_1=target,
    )
    assert transition(signal, 100, 0) == "CANCELLED"

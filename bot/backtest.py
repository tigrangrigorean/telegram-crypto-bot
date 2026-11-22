import argparse
import asyncio
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import numpy as np
import pandas as pd

from .config import cfg
from .db import HistoricalTrade, Run, engine, sessions
from .market import MS, PublicAPI
from .paper import result, sizing
from .strategy import indicators, setup, transition


async def history(api, symbol, tf, start, end):
    parts = []
    cursor = end
    while cursor >= start:
        f = await api.bars(symbol, tf, cursor, 1000)
        if f.empty:
            break
        parts.append(f)
        earliest = int(f.time.min())
        if earliest >= cursor:
            raise ValueError("BingX history cursor stalled")
        cursor = earliest
        if earliest <= start:
            break
    if not parts:
        raise ValueError(f"No history for {symbol} {tf}")
    return (
        pd.concat(parts)
        .drop_duplicates("time")
        .sort_values("time")
        .reset_index(drop=True)
    )


def simulate(frames, precision, start, end, side="both"):
    frames = {tf: indicators(f) for tf, f in frames.items()}
    times = {tf: f.time.to_numpy() + MS[tf] for tf, f in frames.items()}
    pending = None
    opened = None
    balance = cfg.balance
    trades = []
    cooldown_until = 0
    for i, b in enumerate(frames["5m"].itertuples()):
        ts = int(b.time)
        if ts < start or ts + MS["5m"] > end:
            continue
        if pending and opened is None:
            d = 1 if pending.direction == "LONG" else -1
            trigger = (
                max(b.open, pending.entry_price)
                if d == 1
                else min(b.open, pending.entry_price)
            )
            crossed = b.high >= trigger if d == 1 else b.low <= trigger
            if ts >= pending.deadline or d * (b.open - pending.stop_loss) <= 0:
                pending = None
            elif crossed:
                if transition(pending, trigger, 0) == "ENTRY CONFIRMED":
                    entry, qty = sizing(balance, balance, trigger, pending.stop_loss, d)
                    opened = (ts, entry, qty, d)
                else:
                    pending = None
            elif b.low <= pending.stop_loss if d == 1 else b.high >= pending.stop_loss:
                pending = None
        if opened:
            t, entry, qty, d = opened
            sl, tp = pending.stop_loss, pending.take_profit_1
            stop = b.low <= sl if d == 1 else b.high >= sl
            target = b.high >= tp if d == 1 else b.low <= tp
            if stop or target or i == len(frames["5m"]) - 1:
                exit_price = (
                    (min(b.open, sl) if d == 1 else max(b.open, sl))
                    if stop
                    else tp
                    if target
                    else b.close
                )
                exit_fill, _, pnl = result(entry, exit_price, qty, d, cfg.leverage)
                balance += pnl
                trades.append(
                    dict(
                        direction=pending.direction,
                        entry_price=entry,
                        exit_price=exit_fill,
                        pnl=pnl,
                        opened_at=datetime.fromtimestamp(t / 1000, UTC),
                        closed_at=datetime.fromtimestamp((ts + MS["5m"]) / 1000, UTC),
                    )
                )
                pending = opened = None
        decision = ts + MS["5m"]
        if not pending and balance > 0 and decision >= cooldown_until:
            view = {}
            for tf in MS:
                n = np.searchsorted(times[tf], decision, side="right")
                view[tf] = frames[tf].iloc[max(0, n - 50) : n]
            idea = setup(view, precision, decision)
            if idea and side in ("both", idea["direction"]):
                pending = SimpleNamespace(
                    **idea, status="WAITING", deadline=decision + cfg.expiry * 1000
                )
                cooldown_until = decision + cfg.cooldown * 1000
    return trades


def metrics(trades):
    p = np.array([r["pnl"] for r in trades])
    equity = np.r_[cfg.balance, cfg.balance + p.cumsum()]
    peak = np.maximum.accumulate(equity)
    wins, losses = p[p > 0], p[p < 0]
    return dict(
        total_trades=len(p),
        win_rate=float(len(wins) / len(p)) if len(p) else 0.0,
        net_pnl=float(p.sum()),
        max_drawdown=float(((peak - equity) / peak).max()),
        profit_factor=float(-wins.sum() / losses.sum()) if len(losses) else None,
        average_win=float(wins.mean()) if len(wins) else 0.0,
        average_loss=float(losses.mean()) if len(losses) else 0.0,
        expectancy=float(p.mean()) if len(p) else 0.0,
    )


async def run(args):
    stamp = time.time()
    if args.end:
        until = datetime.fromisoformat(args.end)
        if until.tzinfo is None:
            raise ValueError("--end requires a UTC offset")
        stamp = min(stamp, until.timestamp())
    end = int(stamp * 1000) // MS["5m"] * MS["5m"]
    start = end - args.days * 86400000
    async with httpx.AsyncClient(timeout=20) as http:
        api = PublicAPI(http)
        contract = (await api.instruments())[args.symbol]
        frames = {
            tf: await history(api, args.symbol, tf, start - 110 * MS[tf], end)
            for tf in MS
        }
    for tf, f in frames.items():
        if (
            f.time.min() > start - 50 * MS[tf]
            or end - (f.time.max() + MS[tf]) >= MS[tf]
            or not (np.diff(f.time) == MS[tf]).all()
        ):
            raise ValueError(f"Incomplete historical coverage: {tf}")
    trades = simulate(frames, int(contract["pricePrecision"]), start, end, args.side)
    m = metrics(trades)
    async with sessions.begin() as db:
        r = Run(
            symbol=args.symbol,
            strategy="breakout " + args.side,
            timeframe="5m",
            metrics=m,
            **{
                k: m[k] for k in ("total_trades", "win_rate", "net_pnl", "max_drawdown")
            },
        )
        db.add(r)
        await db.flush()
        db.add_all([HistoricalTrade(backtest_run_id=r.id, **t) for t in trades])
    print(m)
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", help="ISO 8601 end time, e.g. 2026-11-20T00:00:00Z")
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument(
        "--days", type=int, default=7, choices=range(1, 366), metavar="1..365"
    )
    parser.add_argument("--side", choices=["LONG", "SHORT", "both"], default="both")
    asyncio.run(run(parser.parse_args()))

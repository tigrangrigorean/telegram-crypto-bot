import asyncio
import logging
import time
from datetime import timedelta

import httpx
from sqlalchemy import insert, select, update
from sqlalchemy.dialects.postgresql import insert as upsert

from .config import cfg
from .db import Delivery, Event, Instrument, Signal, User, now, sessions
from .market import MS, rank, ticker_spread_bps
from .strategy import aligned, breakout_side, fresh, indicators, setup, transition

ACTIVE = ("WAITING", "ENTRY CONFIRMED")
log = logging.getLogger(__name__)


async def event(db, signal, status, price):
    signal.status, signal.updated_at = status, now()
    record = Event(signal_id=signal.id, event_type=status, event_price=price)
    db.add(record)
    await db.flush()
    if status != "WATCH":
        from sqlalchemy import literal

        recipients = select(literal(record.id), User.id).where(
            User.notifications_enabled
        )
        await db.execute(
            insert(Delivery).from_select(["event_id", "user_id"], recipients)
        )


class Service:
    def __init__(self, api):
        self.api = api
        self.instruments = {}
        self.last_attempt = {}
        self.top = []
        self.baseline = {}
        self.spreads = {}
        self.active = set()
        self.queue = asyncio.Queue(2048)
        self.health = {}
        self.dropped = 0

    def targets(self):
        # Only active signals use trade ticks. Ticker rank changes need no reconnect.
        return tuple(sorted(self.active)[:20])

    def tick(self, symbol, price, ts):
        if (
            symbol in self.active
            and price > 0
            and 0 <= time.time() - ts / 1000 <= cfg.quote_age
        ):
            try:
                self.queue.put_nowait((symbol, price, ts))
            except asyncio.QueueFull:
                self.dropped += 1

    async def discover(self):
        instruments = await self.api.instruments()
        if not instruments:
            raise ValueError("empty contract list")
        async with sessions.begin() as db:
            await db.execute(update(Instrument).values(status="inactive"))
            rows = [
                dict(symbol=s, status="active", details=r, updated_at=now())
                for s, r in instruments.items()
            ]
            stmt = upsert(Instrument).values(rows)
            await db.execute(
                stmt.on_conflict_do_update(
                    index_elements=["symbol"],
                    set_={
                        k: getattr(stmt.excluded, k)
                        for k in ("status", "details", "updated_at")
                    },
                )
            )
        self.instruments = instruments
        self.baseline = {s: v for s, v in self.baseline.items() if s in instruments}
        self.last_attempt = {
            s: t for s, t in self.last_attempt.items() if s in instruments
        }

    async def analyze(self, symbol):
        fast = indicators(await self.api.bars(symbol, "5m"))
        d = breakout_side(fast, time.time() * 1000)
        if not d:
            return
        frames = {"5m": fast}
        for tf in ("15m", "1h"):
            slow = indicators(await self.api.bars(symbol, tf))
            if not fresh(slow, tf, time.time() * 1000) or not aligned(slow.iloc[-1], d):
                return
            frames[tf] = slow
        return setup(
            frames, int(self.instruments[symbol]["pricePrecision"]), time.time() * 1000
        )

    async def collect(self):
        rows = await self.api.tickers()
        self.top = rank(rows, self.instruments, self.baseline)[: cfg.candidates]
        spreads = {}
        for r in rows:
            try:
                symbol, last, ts = (
                    r["symbol"],
                    float(r["lastPrice"]),
                    int(r["closeTime"]),
                )
            except (KeyError, TypeError, ValueError):
                continue
            spread = ticker_spread_bps(r)
            if spread is not None and symbol in self.instruments:
                spreads[symbol] = (spread, ts)
            self.tick(symbol, last, ts)
        self.spreads = spreads

    def batch(self, slot):
        due = [s for s in self.instruments if self.last_attempt.get(s, -1) < slot]
        priority = [
            s
            for s in self.top
            if s in self.instruments and self.last_attempt.get(s, -1) < slot
        ][:8]
        oldest = sorted(due, key=lambda s: (self.last_attempt.get(s, -1), s))
        return list(dict.fromkeys(priority + oldest))[:16]

    async def check(self, symbol):
        self.last_attempt[symbol] = int(time.time() * 1000) // MS["5m"]
        try:
            return symbol, await self.analyze(symbol)
        except (ValueError, KeyError, httpx.HTTPError) as exc:
            log.warning("scan %s: %s", symbol, type(exc).__name__)
            return symbol, None

    async def scan(self):
        slot = int(time.time() * 1000) // MS["5m"]
        workers = asyncio.Semaphore(8)

        async def check(symbol):
            async with workers:
                return await self.check(symbol)

        tasks = [asyncio.create_task(check(symbol)) for symbol in self.batch(slot)]
        try:
            for completed in asyncio.as_completed(tasks):
                symbol, idea = await completed
                if idea:
                    await self.publish(symbol, idea)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        slot = int(time.time() * 1000) // MS["5m"]
        self.health["5m coverage"] = (
            f"{sum(t == slot for t in self.last_attempt.values())}/{len(self.instruments)} attempted"
        )

    async def publish(self, symbol, idea):
        spread, stamp = self.spreads.get(symbol, (float("inf"), 0))
        if (
            spread > cfg.max_spread_bps
            or not 0 <= time.time() - stamp / 1000 <= cfg.quote_age
        ):
            return
        if idea["bar"] + MS["5m"] <= time.time() * 1000 - MS["5m"]:
            return
        async with sessions.begin() as db:
            last = await db.scalar(
                select(Signal)
                .where(Signal.symbol == symbol)
                .order_by(Signal.id.desc())
                .limit(1)
            )
            if last and (
                last.bar == idea["bar"]
                or last.status in ACTIVE
                or now() - last.created_at < timedelta(seconds=cfg.cooldown)
            ):
                return
            idea["symbol"] = symbol
            s = Signal(**idea, status="WATCH")
            db.add(s)
            await db.flush()
            await event(db, s, "WATCH", idea["details"]["current"])
            await event(db, s, "WAITING", idea["details"]["current"])
        self.active.add(symbol)

    async def restore(self):
        async with sessions.begin() as db:
            signals = (
                await db.scalars(
                    select(Signal).where(Signal.status.in_(ACTIVE)).with_for_update()
                )
            ).all()
            for s in signals:
                if (
                    s.status == "WAITING"
                    and (now() - s.created_at).total_seconds() >= cfg.expiry
                ):
                    await event(db, s, "EXPIRED", s.details["current"])
            self.active = {s.symbol for s in signals if s.status in ACTIVE}

    async def monitor(self):
        from .paper import settle

        symbol, price, ts = await self.queue.get()
        if time.time() - ts / 1000 > cfg.quote_age:
            return
        async with sessions.begin() as db:
            s = await db.scalar(
                select(Signal)
                .where(Signal.symbol == symbol, Signal.status.in_(ACTIVE))
                .with_for_update()
            )
            if not s or ts / 1000 <= s.updated_at.timestamp():
                return
            status = transition(s, price, (now() - s.created_at).total_seconds())
            if status:
                await event(db, s, status, price)
                await settle(db, s, status, price, self.instruments.get(symbol, {}))

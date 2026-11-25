import asyncio
import gzip
import json
import logging
import math
import time

import aiohttp
import httpx
import pandas as pd

from .config import cfg

BASE = "https://open-api.bingx.com"
WS = "wss://open-api-swap.bingx.com/swap-market"
MS = {"5m": 300000, "15m": 900000, "1h": 3600000}
log = logging.getLogger(__name__)


class PublicAPI:
    def __init__(self, client):
        self.client = client
        self.lock = asyncio.Lock()
        self.next_request = 0.0

    async def get(self, path, **params):
        for attempt in range(5):
            while True:
                async with self.lock:
                    now = time.monotonic()
                    wait = self.next_request - now
                    if wait <= 0:
                        self.next_request = now + 1 / cfg.rest_requests_per_second
                        break
                await asyncio.sleep(wait)
            delay = 2**attempt
            try:
                r = await self.client.get(BASE + "/openApi/swap/" + path, params=params)
                payload = r.json() if r.is_success else None
                if r.status_code == 429 or (payload and payload.get("code") == 100410):
                    retry_after = r.headers.get("retry-after", "")
                    delay = max(delay, int(retry_after) if retry_after.isdigit() else 5)
                    async with self.lock:
                        self.next_request = max(
                            self.next_request, time.monotonic() + delay
                        )
                r.raise_for_status()
                if payload["code"] != 0:
                    raise ValueError(f"BingX code {payload['code']}")
                return payload["data"]
            except (httpx.HTTPError, ValueError, KeyError):
                if attempt == 4:
                    raise
                await asyncio.sleep(delay)

    async def instruments(self):
        rows = await self.get("v2/quote/contracts")
        return {
            r["symbol"]: r
            for r in rows
            if r.get("status") == 1
            and r.get("currency") == "USDT"
            and r["symbol"].endswith("-USDT")
        }

    async def tickers(self):
        return await self.get("v2/quote/ticker")

    async def bars(self, symbol, interval, end=None, limit=110):
        params = {} if end is None else {"endTime": end}
        rows = await self.get(
            "v3/quote/klines", symbol=symbol, interval=interval, limit=limit, **params
        )
        frame = pd.DataFrame(
            rows, columns=["time", "open", "high", "low", "close", "volume"]
        ).astype(float)
        frame = frame.dropna().drop_duplicates("time").sort_values("time")
        return frame[
            frame.time + MS[interval]
            <= (end if end is not None else time.time() * 1000)
        ].reset_index(drop=True)


def parse(raw):
    if isinstance(raw, bytes):
        raw = gzip.decompress(raw).decode()
    if raw == "Ping":
        return "Pong"
    msg = json.loads(raw)
    if msg.get("code", 0) != 0:
        raise ValueError("subscription rejected")
    if not msg.get("dataType", "").endswith("@trade"):
        return []
    rows = msg["data"]
    if isinstance(rows, dict):
        rows = [rows]
    return sorted(
        ((r["s"], float(r["p"]), int(r["T"])) for r in rows), key=lambda x: x[2]
    )


async def stream(targets, on_tick):
    delay = 1
    async with aiohttp.ClientSession(trust_env=True) as session:
        while True:
            symbols = tuple(targets())
            if not symbols:
                await asyncio.sleep(2)
                continue
            try:
                async with session.ws_connect(
                    WS, heartbeat=20, receive_timeout=35
                ) as ws:
                    for symbol in symbols:
                        await ws.send_json(
                            {
                                "id": symbol,
                                "reqType": "sub",
                                "dataType": symbol + "@trade",
                            }
                        )
                        await asyncio.sleep(0.15)
                    while tuple(targets()) == symbols:
                        data = await ws.receive()
                        if data.type not in (
                            aiohttp.WSMsgType.TEXT,
                            aiohttp.WSMsgType.BINARY,
                        ):
                            raise ConnectionError("stream closed")
                        ticks = parse(data.data)
                        if ticks == "Pong":
                            await ws.send_str("Pong")
                        else:
                            for tick in ticks:
                                on_tick(*tick)
                            if ticks:
                                delay = 1
            except (
                aiohttp.ClientError,
                TimeoutError,
                ConnectionError,
                ValueError,
                OSError,
                KeyError,
            ):
                log.warning("BingX stream reconnect in %ss", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)


def ticker_spread_bps(row):
    try:
        bid, ask = float(row["bidPrice"]), float(row["askPrice"])
    except (KeyError, ValueError, TypeError):
        return None
    if not (0 < bid <= ask and math.isfinite(ask)):
        return None
    return 20000 * ((ask - bid) / (ask + bid))


def rank(rows, instruments, baseline):
    scores = {}
    for row in rows:
        symbol = row["symbol"]
        if symbol not in instruments:
            continue
        try:
            volume, price, high, low, change = (
                float(row[k])
                for k in (
                    "quoteVolume",
                    "lastPrice",
                    "highPrice",
                    "lowPrice",
                    "priceChangePercent",
                )
            )
            if min(volume, price, low) <= 0 or not all(
                math.isfinite(x) for x in (volume, price, high, low, change)
            ):
                continue
            previous = baseline.get(symbol)
            surprise = max(0, volume / previous - 1) if previous else 0
            spread = ticker_spread_bps(row)
            # Cap one-off moves and favor tight, actively traded markets.
            activity = min((abs(change) / 100 + max(0, (high - low) / price)), 0.5)
            liquidity = math.log1p(volume / 100_000)
            cost = 1 + (spread if spread is not None else cfg.max_spread_bps) / 10
            scores[symbol] = liquidity * (activity + min(surprise, 0.25)) / cost
            baseline[symbol] = (
                volume if previous is None else 0.95 * previous + 0.05 * volume
            )
        except (KeyError, ValueError, TypeError):
            continue
    return sorted(scores, key=scores.get, reverse=True)

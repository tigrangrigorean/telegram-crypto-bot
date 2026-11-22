import asyncio
import logging
import time
from pathlib import Path

import httpx
from aiogram import Bot
from sqlalchemy.dialects.postgresql import insert

from .config import cfg
from .db import Account, engine, now, sessions
from .market import PublicAPI, stream
from .service import Service
from .telegram import dispatcher, notify

log = logging.getLogger(__name__)


async def repeat(svc, name, fn, interval):
    delay = 1
    while True:
        try:
            await fn()
            svc.health[name] = now().strftime("%H:%M:%S UTC")
            delay = 1
            await asyncio.sleep(interval)
        except Exception as exc:
            svc.health[name] = "error: " + type(exc).__name__
            log.warning("%s: %s", name, type(exc).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


async def main():
    if not cfg.telegram_bot_token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN in .env")
    async with sessions.begin() as db:
        await db.execute(
            insert(Account)
            .values(id=1, initial_balance=cfg.balance, current_balance=cfg.balance)
            .on_conflict_do_nothing()
        )
    async with (
        Bot(cfg.telegram_bot_token.get_secret_value()) as bot,
        httpx.AsyncClient(timeout=15) as client,
    ):
        svc = Service(PublicAPI(client))
        await svc.restore()

        async def heartbeat():
            Path("/tmp/bot-heartbeat").write_text(str(time.time()))

        jobs = [
            ("discovery", svc.discover, 1800),
            ("ticker", svc.collect, 10),
            ("scanner", svc.scan, cfg.scan_seconds),
            ("restore", svc.restore, 30),
            ("monitor", svc.monitor, 0),
            ("notify", lambda: notify(bot), 0.25),
            ("health", heartbeat, 15),
            ("websocket", lambda: stream(svc.targets, svc.tick), 1),
        ]
        tasks = [asyncio.create_task(repeat(svc, *job)) for job in jobs]
        try:
            await dispatcher(svc).start_polling(bot, close_bot_session=False)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())

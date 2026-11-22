import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from .config import cfg
from .db import Account, Delivery, Event, Position, Run, Signal, Trade, User, sessions

HELP = "/start /stop /status /coins /top /signals /long /short /analyze BTCUSDT /stats /paper /backtest /help"


def message(s, event, price, timestamp):
    return (
        f"{event} · {s.symbol} · {s.direction} · 5m breakout\n"
        f"Цена события {price:g} USDT · вход {s.entry_price:g}\n"
        f"Условие входа — следующая сделка на уровне пробоя или дальше с R/R ≥ {cfg.min_rr:g}.\n"
        f"SL {s.stop_loss:g} · TP1 {s.take_profit_1:g} · TP2 {s.take_profit_2:g}\n"
        f"Плановый R/R 1:{s.details['rr']:.2f} · Score {s.score}/100\n"
        + ", ".join(s.details["reasons"])
        + f"\n{timestamp:%Y-%m-%d %H:%M:%S UTC}"
    )


def dispatcher(svc):
    dp = Dispatcher()

    @dp.message(F.text.startswith("/"))
    async def handle(m):
        words = m.text.split()
        cmd = words[0].split("@")[0]
        async with sessions.begin() as db:
            if cmd in ("/start", "/stop"):
                enabled = cmd == "/start"
                stmt = insert(User).values(
                    chat_id=m.chat.id, notifications_enabled=enabled
                )
                await db.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["chat_id"],
                        set_={"notifications_enabled": enabled},
                    )
                )
                if not enabled:
                    await db.execute(
                        update(Delivery)
                        .where(
                            Delivery.user_id
                            == select(User.id)
                            .where(User.chat_id == m.chat.id)
                            .scalar_subquery()
                        )
                        .values(sent=True)
                    )
                answer = (
                    "Уведомления включены." if enabled else "Уведомления отключены."
                )
            elif cmd == "/status":
                answer = str(svc.health) + f" · пропущено тиков {svc.dropped}"
            elif cmd == "/coins":
                answer = f"Активных инструментов: {len(svc.instruments)}"
            elif cmd == "/top":
                answer = ", ".join(svc.top) or "Ожидается сканирование."
            elif cmd in ("/signals", "/long", "/short"):
                q = select(Signal).order_by(Signal.id.desc()).limit(10)
                if cmd != "/signals":
                    q = q.where(Signal.direction == cmd[1:].upper())
                answer = (
                    "\n".join(
                        f"{s.symbol} {s.direction} {s.entry_price:g} · {s.status}"
                        for s in await db.scalars(q)
                    )
                    or "Сигналов пока нет."
                )
            elif cmd == "/analyze":
                symbol = (words[1] if len(words) > 1 else "").upper().replace(
                    "-", ""
                ).removesuffix("USDT") + "-USDT"
                if symbol in svc.instruments:
                    idea = await svc.analyze(symbol)
                    answer = (
                        f"{symbol} {idea['direction']} · Entry {idea['entry_price']} · SL {idea['stop_loss']} · TP {idea['take_profit_1']}"
                        if idea
                        else f"{symbol}: подтверждённого сетапа по закрытым свечам нет."
                    )
                else:
                    answer = "Укажите активный контракт, например /analyze BTCUSDT"
            elif cmd == "/stats":
                counts = dict(
                    (
                        await db.execute(
                            select(Signal.status, func.count()).group_by(Signal.status)
                        )
                    ).all()
                )
                tp, sl = counts.get("TP HIT", 0), counts.get("SL HIT", 0)
                answer = (
                    f"{counts}\nWin rate TP/(TP+SL): {tp / (tp + sl):.1%}"
                    if tp + sl
                    else f"{counts}\nЗавершённых сделок пока нет."
                )
            elif cmd == "/paper":
                acc = await db.get(Account, 1)
                n, w = (
                    await db.execute(
                        select(
                            func.count(), func.count().filter(Trade.pnl > 0)
                        ).select_from(Trade)
                    )
                ).one()
                opened = await db.scalar(
                    select(func.count())
                    .select_from(Position)
                    .where(Position.status == "OPEN")
                )
                answer = f"Баланс {acc.current_balance:.4f} USDT · PnL {acc.current_balance - acc.initial_balance:+.4f}\nОткрыто {opened} · Закрыто {n} · Win rate {w / n if n else 0:.1%}"
            elif cmd == "/backtest":
                r = await db.scalar(select(Run).order_by(Run.id.desc()).limit(1))
                answer = (
                    f"{r.symbol} {r.strategy} {r.timeframe}\n{r.metrics}"
                    if r
                    else "Сохранённых тестов пока нет."
                )
            else:
                answer = HELP
        await m.answer(answer)

    return dp


async def notify(bot: Bot):
    async with sessions.begin() as db:
        item = (
            await db.execute(
                select(Delivery, User, Event, Signal)
                .join(User, Delivery.user_id == User.id)
                .join(Event, Delivery.event_id == Event.id)
                .join(Signal, Event.signal_id == Signal.id)
                .where(Delivery.sent.is_(False), User.notifications_enabled)
                .order_by(Delivery.id)
                .limit(1)
                .with_for_update(of=Delivery, skip_locked=True)
            )
        ).first()
        if not item:
            return
        delivery, user, e, s = item
        try:
            await bot.send_message(
                user.chat_id, message(s, e.event_type, e.event_price, e.created_at)
            )
        except TelegramForbiddenError:
            user.notifications_enabled = False
        except TelegramBadRequest:
            user.notifications_enabled = False
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after)
            return
        delivery.sent = True

import asyncio
from datetime import timedelta
from decimal import Decimal

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


def price_text(value):
    return format(Decimal(str(value)).normalize(), "f")


def message(s, event, price, timestamp):
    titles = {
        "WAITING": "⏳ Ждём входа",
        "ENTRY CONFIRMED": "✅ Вход подтверждён",
        "CANCELLED": "↩️ Сигнал отменён",
        "EXPIRED": "⌛ Время ожидания вышло",
        "TP HIT": "🎯 Цель достигнута",
        "SL HIT": "🛑 Сработал стоп",
    }
    entry = price_text(s.entry_price)
    observed = price_text(price)
    stop = price_text(s.stop_loss)
    target = price_text(s.take_profit_1)
    long = s.direction == "LONG"
    heading = f"{titles[event]} · {s.symbol} · {s.direction}"
    if event == "WAITING":
        deadline = timestamp + timedelta(seconds=cfg.expiry)
        side = "выше" if long else "ниже"
        body = (
            f"Пробой на 5 мин. Цена закрытия {observed} USDT.\n"
            f"Ждём {entry} или {side} до {deadline:%H:%M:%S} UTC.\n\n"
            f"Стоп {stop} · Цель {target}\n"
            f"Дальняя цель {price_text(s.take_profit_2)}\n"
            f"Потенциал к риску {s.details['rr']:.2f} к 1 (минимум {cfg.min_rr:g} к 1).\n"
            f"Сила сигнала {s.score}/100."
        )
    elif event == "ENTRY CONFIRMED":
        body = f"Цена подтверждения {observed} USDT.\nСтоп {stop} · Цель {target}"
    elif event == "EXPIRED":
        body = f"Бот ждал входа на уровне {entry} USDT. За отведённое время вход не подтвердился."
    elif event == "CANCELLED":
        hit_stop = price <= s.stop_loss if long else price >= s.stop_loss
        reason = (
            "Цена достигла стопа до входа."
            if hit_stop
            else "По этой цене потенциальная прибыль слишком мала относительно риска."
        )
        body = f"{reason}\nЦена отмены {observed} USDT."
    else:
        level = f"Цель {target}" if event == "TP HIT" else f"Стоп {stop}"
        body = f"{level} · Бот зафиксировал {observed} USDT.\nСигнал закрыт."
    return f"{heading}\n\n{body}\n\n{timestamp:%d.%m.%Y %H:%M:%S} UTC"


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

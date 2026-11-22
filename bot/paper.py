import math

from sqlalchemy import func, select

from .config import cfg
from .db import Account, Position, Trade, now


def sizing(balance, free, entry, stop, d, c=cfg):
    fill = entry * (1 + d * c.slip)
    exit_stop = stop * (1 - d * c.slip)
    unit_risk = d * (fill - exit_stop) + c.fee * (fill + exit_stop)
    if unit_risk <= 0 or free <= 0:
        return fill, 0.0
    return fill, min(
        balance * c.risk / unit_risk, free / (fill * (1 / c.leverage + c.fee))
    )


def result(entry, exit_price, qty, d, leverage, c=cfg):
    fill = exit_price * (1 - d * c.slip)
    fees = (entry + fill) * qty * c.fee
    return fill, fees, max(-entry * qty / leverage, d * (fill - entry) * qty - fees)


async def settle(db, s, status, price, contract):
    account = await db.get(Account, 1, with_for_update=True)
    d = 1 if s.direction == "LONG" else -1
    if status == "ENTRY CONFIRMED":
        today = now().replace(hour=0, minute=0, second=0, microsecond=0)
        daily = await db.scalar(
            select(func.coalesce(func.sum(Trade.pnl), 0)).where(
                Trade.closed_at >= today
            )
        )
        if daily <= -(account.current_balance - daily) * cfg.daily_loss:
            return
        reserved = await db.scalar(
            select(
                func.coalesce(
                    func.sum(
                        Position.entry_price
                        * Position.quantity
                        * (1 / Position.leverage + cfg.fee)
                    ),
                    0,
                )
            ).where(Position.status == "OPEN")
        )
        entry, qty = sizing(
            account.current_balance,
            account.current_balance - reserved,
            price,
            s.stop_loss,
            d,
        )
        if "quantityPrecision" not in contract:
            return
        scale = 10 ** int(contract["quantityPrecision"])
        qty = math.floor(qty * scale) / scale
        if (
            qty <= 0
            or qty < float(contract.get("tradeMinQuantity") or 0)
            or qty * entry < float(contract.get("tradeMinUSDT") or 0)
        ):
            return
        db.add(
            Position(
                signal_id=s.id,
                symbol=s.symbol,
                direction=s.direction,
                entry_price=entry,
                quantity=qty,
                leverage=cfg.leverage,
                stop_loss=s.stop_loss,
                take_profit=s.take_profit_1,
                status="OPEN",
            )
        )
    elif status in ("SL HIT", "TP HIT"):
        p = await db.scalar(
            select(Position)
            .where(Position.signal_id == s.id, Position.status == "OPEN")
            .with_for_update()
        )
        if p:
            fill, fees, pnl = result(p.entry_price, price, p.quantity, d, p.leverage)
            account.current_balance += pnl
            p.status, p.closed_at = status, now()
            db.add(
                Trade(
                    position_id=p.id,
                    entry_price=p.entry_price,
                    exit_price=fill,
                    pnl=pnl,
                    fees=fees,
                )
            )

import numpy as np
import pandas as pd

from .config import cfg
from .market import MS


def indicators(frame, c=cfg):
    f = frame.copy()
    close = f.close
    for span in (9, 21, 50):
        f[f"ema{span}"] = close.ewm(span=span, adjust=False).mean()
    ema = lambda n: close.ewm(span=n, adjust=False).mean()
    macd = ema(12) - ema(26)
    f["hist"] = macd - macd.ewm(span=9, adjust=False).mean()
    delta = close.diff()
    avg = lambda x: x.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    gain, loss = avg(delta.clip(lower=0)), avg(-delta.clip(upper=0))
    f["rsi"] = 100 * gain / (gain + loss)
    prev = close.shift()
    f["atr"] = avg(
        pd.concat(
            [f.high - f.low, (f.high - prev).abs(), (f.low - prev).abs()], axis=1
        ).max(axis=1)
    )
    f["rv"] = f.volume / f.volume.shift().rolling(c.window).mean()
    f["res"] = f.high.shift().rolling(c.window).max()
    f["sup"] = f.low.shift().rolling(c.window).min()
    return f


def fresh(frame, tf, end):
    recent = frame.tail(50)
    if len(recent) < 50:
        return False
    row = recent.iloc[-1]
    return (
        0 <= end - row.time - MS[tf] < MS[tf]
        and (np.diff(recent.time) == MS[tf]).all()
        and np.isfinite(row).all()
    )


def breakout_side(frame, end, c=cfg):
    if not fresh(frame, "5m", end):
        return 0
    r = frame.iloc[-1]
    if not (
        r.close > 0
        and r.rv >= c.volume_ratio
        and c.atr_min <= r.atr / r.close <= c.atr_max
        and r.low <= r.close <= r.high
        and r.low < r.high
    ):
        return 0
    if r.close > r.res:
        d, distance, close_strength = 1, r.close - r.res, r.close - r.low
    elif r.close < r.sup:
        d, distance, close_strength = -1, r.sup - r.close, r.high - r.close
    else:
        return 0
    rsi = r.rsi if d == 1 else 100 - r.rsi
    if (
        distance < c.breakout_min_atr * r.atr
        or close_strength < c.breakout_close_fraction * (r.high - r.low)
        or not c.rsi_low <= rsi <= c.rsi_high
        or not aligned(r, d)
    ):
        return 0
    return d


def aligned(row, d):
    return (
        d * (row.ema9 - row.ema21) > 0
        and d * (row.ema21 - row.ema50) > 0
        and d * row["hist"] > 0
    )


def net_rr(entry, stop, target, d, c):
    fill = entry * (1 + d * c.slip)
    loss_exit = stop * (1 - d * c.slip)
    win_exit = target * (1 - d * c.slip)
    risk = d * (fill - loss_exit) + c.fee * (fill + loss_exit)
    reward = d * (win_exit - fill) - c.fee * (fill + win_exit)
    return reward / risk if risk > 0 else 0.0


def setup(frames, precision, end, c=cfg):
    d = breakout_side(frames["5m"], end, c)
    if not d:
        return
    if not all(
        fresh(frames[tf], tf, end) and aligned(frames[tf].iloc[-1], d)
        for tf in ("15m", "1h")
    ):
        return
    direction = "LONG" if d == 1 else "SHORT"
    r = frames["5m"].iloc[-1]
    level = r.res if d == 1 else r.sup
    rnd = lambda v: round(float(v), precision)
    entry = rnd(r.high + 10**-precision if d == 1 else r.low - 10**-precision)
    stop = rnd(level - d * c.stop_atr * r.atr)
    width = r.res - r.sup
    tp1, tp2 = rnd(level + d * width), rnd(level + d * 1.5 * width)
    risk = d * (entry - stop)
    rr = net_rr(entry, stop, tp1, d, c)
    if min(entry, stop, tp1, tp2) <= 0 or risk <= 0 or rr < c.min_rr:
        return
    return dict(
        symbol=None,
        bar=int(r.time),
        direction=direction,
        entry_price=entry,
        stop_loss=stop,
        take_profit_1=tp1,
        take_profit_2=tp2,
        score=min(
            100,
            70
            + int(
                15 * min(r.rv / (2 * c.volume_ratio), 1)
                + 15 * min(d * (r.close - level) / r.atr, 1)
            ),
        ),
        details=dict(
            current=rnd(r.close),
            rr=rr,
            timeframe="5m",
            reasons=[
                "5m/15m/1h EMA and MACD trend",
                "Closed breakout",
                "Relative volume and RSI confirmed",
            ],
        ),
    )


def transition(s, price, age, c=cfg):
    d = 1 if s.direction == "LONG" else -1
    if s.status == "WAITING":
        if age >= c.expiry:
            return "EXPIRED"
        if d * (price - s.stop_loss) <= 0:
            return "CANCELLED"
        if d * (price - s.entry_price) >= 0:
            return (
                "ENTRY CONFIRMED"
                if net_rr(price, s.stop_loss, s.take_profit_1, d, c) >= c.min_rr
                else "CANCELLED"
            )
    if s.status == "ENTRY CONFIRMED":
        if d * (price - s.stop_loss) <= 0:
            return "SL HIT"
        if d * (price - s.take_profit_1) >= 0:
            return "TP HIT"

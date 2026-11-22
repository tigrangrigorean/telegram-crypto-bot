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


def setup(frames, precision, end, c=cfg):
    rows = []
    for tf in MS:
        f = frames[tf]
        if len(f) < 50:
            return
        recent = f.tail(50)
        r = recent.iloc[-1]
        if (
            not (0 <= end - r.time - MS[tf] < MS[tf])
            or not (np.diff(recent.time) == MS[tf]).all()
        ):
            return
        if not np.isfinite(r).all():
            return
        rows.append(r)
    r = rows[0]
    for d, direction in [(1, "LONG"), (-1, "SHORT")]:
        if not all(
            d * (x.ema9 - x.ema21) > 0
            and d * (x.ema21 - x.ema50) > 0
            and d * x["hist"] > 0
            for x in rows
        ):
            continue
        level = r.res if d == 1 else r.sup
        strength = r.rsi if d == 1 else 100 - r.rsi
        if not (
            d * (r.close - level) > 0
            and c.rsi_low <= strength <= c.rsi_high
            and r.rv >= c.volume_ratio
            and c.atr_min <= r.atr / r.close <= c.atr_max
        ):
            continue
        rnd = lambda v: round(float(v), precision)
        entry = rnd(r.high + 10**-precision if d == 1 else r.low - 10**-precision)
        stop = rnd(level - d * c.stop_atr * r.atr)
        width = r.res - r.sup
        tp1, tp2 = rnd(level + d * width), rnd(level + d * 1.5 * width)
        risk = d * (entry - stop)
        if (
            min(entry, stop, tp1, tp2) <= 0
            or risk <= 0
            or d * (tp1 - entry) / risk < c.min_rr
        ):
            continue
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
                rr=d * (tp1 - entry) / risk,
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
            reward = d * (s.take_profit_1 - price)
            return (
                "ENTRY CONFIRMED"
                if reward >= c.min_rr * d * (price - s.stop_loss) - 1e-12
                else "CANCELLED"
            )
    if s.status == "ENTRY CONFIRMED":
        if d * (price - s.stop_loss) <= 0:
            return "SL HIT"
        if d * (price - s.take_profit_1) >= 0:
            return "TP HIT"

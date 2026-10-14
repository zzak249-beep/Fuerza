#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Escáner Granville (G1/G3/G4/G5/G7/G8) para BingX Perpetual Futures -> Telegram.

SOLO ENVÍA SEÑALES. No opera, no usa claves de BingX (solo datos públicos de mercado).
La lógica replica la estrategia/indicador de TradingView "Granville Adaptativo v3".

v2: además de avisar, SIGUE cada señal (TP1 parcial, break-even, trailing, TP2, SL, time-stop,
señal contraria) como la estrategia de TradingView, te avisa de cada hito y calcula estadísticas
reales en R. Incluye backtester con la misma lógica para medir qué señales tienen ventaja.

Uso rápido:
    python bot.py --backtest top:30 --days 60 # backtest en las 30 monedas más líquidas
    python bot.py --list                      # lista los contratos que escaneará
    python bot.py --test-telegram             # prueba el envío a Telegram
    python bot.py --debug BTC-USDT --tf 15m   # valores de filtros de la última vela
    python bot.py --history BTC-USDT --tf 15m --bars 200   # señales de las últimas N velas
    python bot.py --once --dry-run            # un escaneo completo, imprime en consola
    python bot.py                             # modo continuo (escanea al cierre de cada vela)
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import numpy as np
import pandas as pd
import requests

BASE_URL = "https://open-api.bingx.com"
TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}
KIND_LABEL = {
    "G1": "Cruce alcista", "G3": "Rebote en la media", "G4": "Sobreventa extrema",
    "G5": "Cruce bajista", "G7": "Rechazo en la media", "G8": "Sobrecompra extrema",
}
TV_INTERVAL = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30", "1h": "60", "2h": "120", "4h": "240", "1d": "D"}
KIND_SIDE = {"G1": "LONG", "G3": "LONG", "G4": "LONG", "G5": "SHORT", "G7": "SHORT", "G8": "SHORT"}

log = logging.getLogger("granville")


# ============================================================
# CONFIGURACIÓN (variables de entorno o fichero .env)
# ============================================================
def load_env(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def make_cfg() -> SimpleNamespace:
    e = os.getenv

    def s(name, d):
        return e(name, str(d)).strip()

    def f(name, d):
        return float(s(name, d))

    def i(name, d):
        return int(float(s(name, d)))

    def b(name, d):
        return s(name, d).lower() in ("1", "true", "yes", "si", "sí", "on")

    htf_map = {}
    for part in s("HTF_MAP", "5m:1h,15m:4h").split(","):
        if ":" in part:
            k, v = part.split(":", 1)
            htf_map[k.strip()] = v.strip()

    return SimpleNamespace(
        token=s("TELEGRAM_TOKEN", ""), chat_id=s("TELEGRAM_CHAT_ID", ""),
        timeframes=[t.strip() for t in s("TIMEFRAMES", "15m,5m").split(",") if t.strip()],
        quotes=[q.strip().upper() for q in s("QUOTE_ASSETS", "USDT").split(",") if q.strip()],
        include_re=s("INCLUDE_REGEX", ""), exclude_re=s("EXCLUDE_REGEX", ""),
        min_volume=f("MIN_VOLUME_24H_USDT", 2_000_000),
        candles=i("CANDLES", 300),
        # media y señales
        ma_length=i("MA_LENGTH", 50), ma_type=s("MA_TYPE", "EMA").upper(),
        atr_length=i("ATR_LENGTH", 14), strength_mult=f("STRENGTH_MULT", 1.2),
        dev_mult=f("DEV_MULT", 2.5), touch_mult=f("TOUCH_MULT", 0.15),
        slope_len=i("SLOPE_LEN", 3), min_slope=f("MIN_SLOPE", 0.02),
        direction=s("DIRECTION", "both").lower(),
        signal_types=[x.strip().upper() for x in s("SIGNAL_TYPES", "G1,G3,G4,G5,G7,G8").split(",")],
        # régimen
        regime=s("REGIME_MODE", "ER").upper(), er_len=i("ER_LEN", 20), er_min=f("ER_MIN", 0.25),
        adx_len=i("ADX_LEN", 14), adx_min=f("ADX_MIN", 22),
        # contexto
        use_htf=b("USE_HTF", 1), htf_map=htf_map, htf_len=i("HTF_LEN", 50),
        htf_slope_req=b("HTF_SLOPE_REQ", 0), use_prev_day=b("USE_PREV_DAY", 0),
        vol_floor=f("VOL_FLOOR", 0.7),
        # volumen / vela
        use_vol=b("USE_VOL", 1), rvol_min=f("RVOL_MIN", 1.0), climax_mult=f("CLIMAX_MULT", 1.8),
        wick_pct=f("WICK_PCT", 0.4), body_ratio_min=f("BODY_RATIO_MIN", 0.55),
        use_delta=b("USE_DELTA", 1), delta_min=f("DELTA_MIN", 0.10),
        # costes
        cost_mult=f("COST_MULT", 5.0), comm_pct=f("COMM_PCT", 0.05), slip_ticks=f("SLIP_TICKS", 2),
        # niveles y tamaño sugerido
        sl_mult=f("SL_MULT", 1.5), tp1_r=f("TP1_R", 1.0),
        long_tp2_r=f("LONG_TP2_R", 3.0), short_tp2_r=f("SHORT_TP2_R", 2.0),
        account=f("ACCOUNT_USDT", 1000), risk_pct=f("RISK_PCT", 1.0), max_lev=f("MAX_LEV", 3.0),
        long_risk_mult=f("LONG_RISK_MULT", 1.0), short_risk_mult=f("SHORT_RISK_MULT", 0.75),
        # gestión de la operación (igual que la estrategia de TradingView)
        partial_pct=f("PARTIAL_PCT", 50), trail_mult=f("TRAIL_MULT", 1.0),
        use_time_stop=b("USE_TIME_STOP", 1), max_bars=i("MAX_BARS", 40), exit_on_opp=b("EXIT_ON_OPP", 1),
        # seguimiento, límites y avisos
        track=b("TRACK_TRADES", 1), max_open=i("MAX_OPEN_TRADES", 8),
        commands=b("TELEGRAM_COMMANDS", 1), summary_hour=i("SUMMARY_HOUR_UTC", 8),
        scan_on_start=b("SCAN_ON_START", 0), trades_file=s("TRADES_FILE", ""),
        # ejecución
        rps=f("RATE_LIMIT_RPS", 8), workers=i("WORKERS", 8),
        delay=f("SCAN_DELAY_SEC", 6), max_age_factor=f("MAX_SIGNAL_AGE_FACTOR", 1.5),
        max_per_scan=i("MAX_SIGNALS_PER_SCAN", 8), state_file=s("STATE_FILE", "state.json"),
    )


# ============================================================
# INDICADORES (equivalentes a Pine v6)
# ============================================================
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def rma(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    pc = df["close"].shift(1)
    return pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)


def compute_frame(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Calcula indicadores y banderas de señal para TODAS las velas del DataFrame."""
    o, h, l, c, v = df["open"], df["high"], df["low"], df["close"], df["volume"]
    ma = sma(c, cfg.ma_length) if cfg.ma_type == "SMA" else ema(c, cfg.ma_length)
    tr = true_range(df)
    atr = rma(tr, cfg.atr_length)
    slope = (ma - ma.shift(cfg.slope_len)) / cfg.slope_len / atr
    rising = slope > cfg.min_slope
    falling = slope < -cfg.min_slope

    body = (c - o).abs()
    avg_body = sma(body, 20)
    strong = body > avg_body * cfg.strength_mult
    bull, bear = c > o, c < o
    rng = h - l
    rng_safe = rng.replace(0, np.nan)
    body_ratio = (body / rng_safe).fillna(0.0)
    lower_wick = np.minimum(o, c) - l
    upper_wick = h - np.maximum(o, c)
    hammer = (lower_wick / rng_safe).fillna(0.0) >= cfg.wick_pct
    shoot = (upper_wick / rng_safe).fillna(0.0) >= cfg.wick_pct

    dist = (c - ma) / atr
    tol = atr * cfg.touch_mult

    # régimen: ER de Kaufman y/o ADX
    er_den = c.diff().abs().rolling(cfg.er_len).sum()
    er = ((c - c.shift(cfg.er_len)).abs() / er_den.replace(0, np.nan)).fillna(0.0)
    up, dn = h.diff(), -l.diff()
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    atr_adx = rma(tr, cfg.adx_len)
    pdi = 100 * rma(plus_dm, cfg.adx_len) / atr_adx
    mdi = 100 * rma(minus_dm, cfg.adx_len) / atr_adx
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    adx = rma(dx.fillna(0.0), cfg.adx_len)
    er_ok, adx_ok = er >= cfg.er_min, adx > cfg.adx_min
    trend_ok = er_ok if cfg.regime == "ER" else adx_ok if cfg.regime == "ADX" else (er_ok & adx_ok)

    rvol = v / sma(v, 20).replace(0, np.nan)
    if cfg.use_vol:
        rvol_ok, climax = rvol >= cfg.rvol_min, rvol >= cfg.climax_mult
    else:
        rvol_ok = climax = pd.Series(True, index=df.index)
    vol_ok = atr > sma(atr, 50) * cfg.vol_floor

    cross_up = (c > ma) & (c.shift(1) <= ma.shift(1))
    cross_dn = (c < ma) & (c.shift(1) >= ma.shift(1))
    buy_cross = cross_up & ~falling & strong & bull
    buy_bounce = (l <= ma + tol) & (c > ma) & rising & strong & bull & ~buy_cross
    buy_ext = (dist < -cfg.dev_mult) & ~rising & bull & (hammer | strong)
    sell_cross = cross_dn & ~rising & strong & bear
    sell_bounce = (h >= ma - tol) & (c < ma) & falling & strong & bear & ~sell_cross
    sell_ext = (dist > cfg.dev_mult) & ~falling & bear & (shoot | strong)

    return pd.DataFrame({
        "time": df["time"], "close": c, "volume": v, "ma": ma, "atr": atr, "slope": slope,
        "er": er, "adx": adx, "rvol": rvol, "bodyRatio": body_ratio, "dist": dist,
        "trendOk": trend_ok, "rvolOk": rvol_ok, "climax": climax, "volOk": vol_ok,
        "bodyOk": body_ratio >= cfg.body_ratio_min,
        "buyCross": buy_cross, "buyBounce": buy_bounce, "buyExt": buy_ext,
        "sellCross": sell_cross, "sellBounce": sell_bounce, "sellExt": sell_ext,
    })


def base_candidates(f: pd.DataFrame, i: int, cfg, tick: float) -> list[tuple[str, str, str]]:
    """Señales de la vela i que pasan los filtros 'baratos'. Devuelve (kind, side, mode)."""
    r = f.iloc[i]
    if any(pd.isna(r[k]) for k in ("ma", "atr", "slope")):
        return []
    if not bool(r["volOk"]):
        return []
    price, atr = float(r["close"]), float(r["atr"])
    cost_pct = 2 * cfg.comm_pct + 2 * cfg.slip_ticks * tick / price * 100
    stop_pct = atr * cfg.sl_mult / price * 100
    if cfg.cost_mult > 0 and stop_pct < cfg.cost_mult * cost_pct:
        return []

    out = []
    cont_ok = bool(r["trendOk"] and r["rvolOk"] and r["bodyOk"])
    spec = (
        ("LONG", "buyCross", "buyBounce", "buyExt", "G1", "G3", "G4"),
        ("SHORT", "sellCross", "sellBounce", "sellExt", "G5", "G7", "G8"),
    )
    for side, kc, kb, ke, gc, gb, ge in spec:
        if cfg.direction == "long" and side == "SHORT":
            continue
        if cfg.direction == "short" and side == "LONG":
            continue
        if cont_ok:
            if bool(r[kc]) and gc in cfg.signal_types:
                out.append((gc, side, "cont"))
            if bool(r[kb]) and gb in cfg.signal_types:
                out.append((gb, side, "cont"))
        if bool(r[ke]) and bool(r["climax"]) and ge in cfg.signal_types:
            out.append((ge, side, "ext"))
    return out


# ============================================================
# API BINGX (datos públicos)
# ============================================================
class BingX:
    def __init__(self, rps: float, timeout: float = 15.0):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "granville-scanner/1.0"})
        self.timeout = timeout
        self.min_interval = 1.0 / max(rps, 0.5)
        self.next_t = 0.0
        self.lock = threading.Lock()

    def _wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next_t)
            self.next_t = t + self.min_interval
        if t > now:
            time.sleep(t - now)

    def get(self, path: str, params: Optional[dict] = None, retries: int = 4):
        last: Optional[Exception] = None
        for attempt in range(retries):
            self._wait()
            try:
                r = self.session.get(BASE_URL + path, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    time.sleep(2 + 3 * attempt)
                    last = RuntimeError("HTTP 429 (límite de peticiones)")
                    continue
                r.raise_for_status()
                j = r.json()
                if j.get("code", 0) != 0:
                    raise RuntimeError(f"BingX code={j.get('code')} msg={j.get('msg')}")
                return j.get("data")
            except Exception as ex:  # noqa: BLE001
                last = ex
                time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"Fallo en {path}: {last}")

    def contracts(self, quotes: list[str]) -> list[dict]:
        data = self.get("/openApi/swap/v2/quote/contracts") or []
        out = []
        for c in data:
            sym = str(c.get("symbol", ""))
            if "-" not in sym or str(c.get("status", "1")) != "1":
                continue
            if sym.split("-")[-1].upper() not in quotes:
                continue
            out.append(c)
        return out

    def volumes_24h(self) -> dict[str, float]:
        data = self.get("/openApi/swap/v2/quote/ticker") or []
        vols = {}
        for t in data:
            try:
                q = t.get("quoteVolume")
                vols[t["symbol"]] = float(q) if q not in (None, "") else float(t.get("volume", 0)) * float(t.get("lastPrice", 0))
            except Exception:  # noqa: BLE001
                continue
        return vols

    def klines(self, symbol: str, interval: str, limit: int, end_time: Optional[int] = None) -> pd.DataFrame:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if end_time:
            params["endTime"] = int(end_time)
        data = self.get("/openApi/swap/v3/quote/klines", params)
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        need = ["time", "open", "high", "low", "close", "volume"]
        if not all(k in df.columns for k in need):
            raise RuntimeError(f"Formato de klines inesperado: {list(df.columns)}")
        df = df[need].astype(float)
        df["time"] = df["time"].astype("int64")
        return df.sort_values("time").drop_duplicates("time").reset_index(drop=True)


def closed_only(df: pd.DataFrame, interval: str, now_ms: Optional[int] = None) -> pd.DataFrame:
    """Descarta la vela en formación: solo velas ya cerradas."""
    if df.empty:
        return df
    now_ms = now_ms or int(time.time() * 1000)
    return df[df["time"] + TF_MS[interval] <= now_ms].reset_index(drop=True)


# ============================================================
# FILTROS CON DATOS EXTRA (solo se piden si hay candidato)
# ============================================================
class Extras:
    """Descarga perezosa y cacheada de HTF, 1m y día previo para un símbolo."""

    def __init__(self, api: BingX, symbol: str, tf: str, cfg):
        self.api, self.symbol, self.tf, self.cfg = api, symbol, tf, cfg
        self._cache: dict[str, pd.DataFrame] = {}

    def _kl(self, interval: str, limit: int) -> pd.DataFrame:
        key = f"{interval}:{limit}"
        if key not in self._cache:
            self._cache[key] = closed_only(self.api.klines(self.symbol, interval, limit), interval)
        return self._cache[key]

    def htf_state(self) -> Optional[dict]:
        htf = self.cfg.htf_map.get(self.tf)
        if not htf or htf not in TF_MS:
            return None
        df = self._kl(htf, self.cfg.htf_len * 2 + 20)
        if len(df) < self.cfg.htf_len + 3:
            return {"tf": htf, "bull": False, "bear": False, "n/d": True}
        e = ema(df["close"], self.cfg.htf_len)
        close, m, m_prev = float(df["close"].iloc[-1]), float(e.iloc[-1]), float(e.iloc[-2])
        up_slope = (not self.cfg.htf_slope_req) or m > m_prev
        dn_slope = (not self.cfg.htf_slope_req) or m < m_prev
        return {"tf": htf, "bull": close > m and up_slope, "bear": close < m and dn_slope}

    def delta_pct(self, candle_open_ms: int, tf_volume: float) -> Optional[float]:
        tf_min = TF_MS[self.tf] // 60_000
        df = self._kl("1m", tf_min + 10)
        win = df[(df["time"] >= candle_open_ms) & (df["time"] < candle_open_ms + TF_MS[self.tf])]
        if len(win) < max(1, int(tf_min * 0.8)) or tf_volume <= 0:
            return None
        delta = float((np.sign(win["close"] - win["open"]) * win["volume"]).sum())
        return delta / tf_volume

    def prev_day(self) -> Optional[tuple[float, float]]:
        df = self._kl("1d", 5)
        if df.empty:
            return None
        return float(df["high"].iloc[-1]), float(df["low"].iloc[-1])


# ============================================================
# MOTOR DE OPERACIÓN (idéntico en seguimiento en vivo y en backtest)
# Réplica de la gestión de la estrategia de TradingView:
#   SL = ATR x SL_MULT · TP1 parcial (PARTIAL_PCT) · SL a break-even tras TP1
#   trailing de TRAIL_MULT R sobre el resto · TP2 · time-stop · cierre por señal contraria
# Si en una misma vela se tocan SL y TP1, se asume el SL primero (conservador).
# ============================================================
def levels(side: str, price: float, stop_dist: float, cfg) -> tuple[float, float, float, float]:
    sgn = 1 if side == "LONG" else -1
    tp2_r = cfg.long_tp2_r if side == "LONG" else cfg.short_tp2_r
    return (price - sgn * stop_dist, price + sgn * stop_dist * cfg.tp1_r,
            price + sgn * stop_dist * tp2_r, tp2_r)


def make_trade(sym: str, tf: str, side: str, kinds: list, ms: int, price: float, stop_dist: float,
               prec: int, cfg) -> dict:
    sl, tp1, tp2, tp2_r = levels(side, price, stop_dist, cfg)
    tick = 10 ** (-prec)
    cost_r = (2 * cfg.comm_pct / 100 * price + 2 * cfg.slip_ticks * tick) / stop_dist if stop_dist > 0 else 0.0
    return {
        "id": f"{sym}|{tf}|{ms}|{side}", "symbol": sym, "tf": tf, "side": side, "kinds": list(kinds),
        "sig_ms": ms, "last_ms": ms, "entry": price, "risk": stop_dist, "sl": sl, "tp1": tp1, "tp2": tp2,
        "tp2_r": tp2_r, "prec": prec, "p": cfg.partial_pct / 100.0, "stage": 1, "bars": 0, "ext": price,
        "realized": 0.0, "cost_r": cost_r, "closed": False,
    }


def _stop_hit(s: int, bar: dict, level: float) -> bool:
    return bar["low"] <= level if s > 0 else bar["high"] >= level


def _target_hit(s: int, bar: dict, level: float) -> bool:
    return bar["high"] >= level if s > 0 else bar["low"] <= level


def close_at(tr: dict, price: float, reason: str, exit_ms: int) -> None:
    s = 1 if tr["side"] == "LONG" else -1
    rem = 1.0 if tr["stage"] == 1 else 1.0 - tr["p"]
    tr["realized"] += rem * s * (price - tr["entry"]) / tr["risk"]
    tr.update(closed=True, reason=reason, exit_ms=exit_ms, exit_px=price, R=tr["realized"] - tr["cost_r"])


def step_trade(tr: dict, bar: dict, cfg) -> list[str]:
    """Avanza la operación con una vela cerrada. Devuelve eventos: TP1, TP2, SL, BE, TRAIL, TIME."""
    ev: list[str] = []
    s = 1 if tr["side"] == "LONG" else -1
    end_ms = int(bar["time"]) + TF_MS[tr["tf"]]
    tr["bars"] += 1
    if tr["stage"] == 1:
        if _stop_hit(s, bar, tr["sl"]):
            close_at(tr, tr["sl"], "SL", end_ms)
            return ["SL"]
        if _target_hit(s, bar, tr["tp1"]):
            tr["realized"] += tr["p"] * s * (tr["tp1"] - tr["entry"]) / tr["risk"]
            tr["stage"] = 2
            tr["ext"] = bar["high"] if s > 0 else bar["low"]
            ev.append("TP1")
            if _target_hit(s, bar, tr["tp2"]):
                close_at(tr, tr["tp2"], "TP2", end_ms)
                ev.append("TP2")
                return ev
    else:
        trail = tr["ext"] - s * tr["risk"] * cfg.trail_mult
        stop = max(tr["entry"], trail) if s > 0 else min(tr["entry"], trail)
        if _stop_hit(s, bar, stop):
            reason = "BE" if abs(stop - tr["entry"]) < 1e-12 else "TRAIL"
            close_at(tr, stop, reason, end_ms)
            return ev + [reason]
        if _target_hit(s, bar, tr["tp2"]):
            close_at(tr, tr["tp2"], "TP2", end_ms)
            return ev + ["TP2"]
        tr["ext"] = max(tr["ext"], bar["high"]) if s > 0 else min(tr["ext"], bar["low"])
    if cfg.use_time_stop and tr["bars"] >= cfg.max_bars:
        close_at(tr, float(bar["close"]), "TIME", end_ms)
        ev.append("TIME")
    return ev


# ============================================================
# ESTADÍSTICAS
# ============================================================
def summarize(rows: list[dict]) -> Optional[dict]:
    if not rows:
        return None
    rows = sorted(rows, key=lambda r: r.get("exit_ms", 0))
    rs = np.array([float(r["R"]) for r in rows])
    n = len(rs)
    win = rs > 0
    gp, gl = float(rs[win].sum()), float(-rs[~win].sum())
    sd = float(rs.std(ddof=1)) if n > 1 else 0.0
    eq = np.r_[0.0, np.cumsum(rs)]
    return {
        "n": n, "wr": float(win.mean()), "avg_r": float(rs.mean()), "total_r": float(rs.sum()),
        "pf": gp / gl if gl > 0 else float("inf"), "t": float(rs.mean() / (sd / np.sqrt(n))) if sd > 1e-6 else 0.0,
        "maxdd": float((np.maximum.accumulate(eq) - eq).max()),
        "avg_win": float(rs[win].mean()) if win.any() else 0.0,
        "avg_loss": float(rs[~win].mean()) if (~win).any() else 0.0,
    }


def verdict(sm: Optional[dict]) -> str:
    if not sm:
        return "sin datos"
    if sm["n"] < 30:
        return "muestra insuficiente (menos de 30 ops)"
    if sm["t"] >= 2.0:
        return "ventaja probable"
    if sm["t"] <= -2.0:
        return "ventaja NEGATIVA"
    return "sin evidencia de ventaja"


def fmt_pf(x: float) -> str:
    return "∞" if x == float("inf") else f"{x:.2f}"


def group_by(rows: list[dict], keyf) -> dict[str, list[dict]]:
    g: dict[str, list[dict]] = {}
    for r in rows:
        g.setdefault(keyf(r), []).append(r)
    return g


# ============================================================
# SEGUIMIENTO DE OPERACIONES VIRTUALES (paper trading de cada señal enviada)
# ============================================================
def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


class Tracker:
    def __init__(self, path: str, cfg):
        self.path, self.cfg = Path(path), cfg
        self.lock = threading.Lock()
        self.open: list[dict] = []
        self.closed: list[dict] = []
        if self.path.exists():
            try:
                d = json.loads(self.path.read_text(encoding="utf-8"))
                self.open, self.closed = d.get("open", []), d.get("closed", [])
            except Exception as ex:  # noqa: BLE001
                log.warning("No se pudo leer %s (%s): empiezo vacío", self.path, ex)

    def save(self) -> None:
        with self.lock:
            self.closed = self.closed[-3000:]
            _atomic_write(self.path, json.dumps({"open": self.open, "closed": self.closed}))

    def open_count(self) -> int:
        with self.lock:
            return len(self.open)

    def has_open(self, symbol: str, tf: str) -> bool:
        with self.lock:
            return any(t["symbol"] == symbol and t["tf"] == tf for t in self.open)

    def add(self, s: "Signal") -> None:
        tr = make_trade(s.symbol, s.tf, s.side, s.kinds, s.candle_open_ms, s.price, s.stop_dist, s.prec, self.cfg)
        with self.lock:
            if not any(t["id"] == tr["id"] for t in self.open):
                self.open.append(tr)

    def _finish(self, tr: dict) -> None:
        with self.lock:
            self.open = [t for t in self.open if t["id"] != tr["id"]]
            self.closed.append(tr)

    def update(self, symbol: str, tf: str, df: pd.DataFrame, sigs: list) -> list[str]:
        """Avanza las operaciones abiertas de (symbol, tf) con las velas cerradas nuevas."""
        with self.lock:
            mine = [t for t in self.open if t["symbol"] == symbol and t["tf"] == tf]
        if not mine or df is None or df.empty:
            return []
        msgs: list[str] = []
        for tr in mine:
            events: list[str] = []
            for row in df[df["time"] > tr["last_ms"]].itertuples(index=False):
                bar = {"time": row.time, "open": row.open, "high": row.high, "low": row.low, "close": row.close}
                events += step_trade(tr, bar, self.cfg)
                tr["last_ms"] = int(row.time)
                if tr["closed"]:
                    break
            if not tr["closed"] and self.cfg.exit_on_opp:
                if any(s.side != tr["side"] and s.candle_open_ms == tr["last_ms"] for s in sigs):
                    close_at(tr, float(df["close"].iloc[-1]), "SIGNAL", tr["last_ms"] + TF_MS[tf])
                    events.append("SIGNAL")
            for e in events:
                msgs.append(build_event_message(tr, e, self.cfg))
            if tr["closed"]:
                self._finish(tr)
        return msgs

    def expire_stale(self, now_ms: int) -> list[str]:
        """Cierra operaciones sin datos (p. ej. contrato retirado) tras el doble de su vida máxima."""
        msgs = []
        with self.lock:
            stale = [t for t in self.open if now_ms - t["last_ms"] > (self.cfg.max_bars * 2 + 5) * TF_MS[t["tf"]]]
        for tr in stale:
            close_at(tr, tr["entry"], "EXPIRED", now_ms)
            tr["R"] = tr["realized"]
            msgs.append(build_event_message(tr, "EXPIRED", self.cfg))
            self._finish(tr)
        return msgs

    def closed_since(self, since_ms: Optional[int]) -> list[dict]:
        with self.lock:
            return [t for t in self.closed if since_ms is None or t.get("exit_ms", 0) >= since_ms]


# ============================================================
# SEÑALES
# ============================================================
@dataclass
class Signal:
    symbol: str
    tf: str
    side: str
    kinds: list
    candle_open_ms: int
    price: float
    atr: float
    sl: float
    tp1: float
    tp2: float
    stop_dist: float
    prec: int
    stats: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.tf}|{self.candle_open_ms}|{self.side}"


def analyze_symbol(api: BingX, contract: dict, tf: str, cfg, now_ms: int) -> tuple[list[Signal], pd.DataFrame]:
    """Devuelve (señales de la última vela cerrada, velas cerradas) para poder seguir operaciones abiertas."""
    sym = contract["symbol"]
    prec = int(contract.get("pricePrecision", 6) or 6)
    tick = 10 ** (-prec)
    df = closed_only(api.klines(sym, tf, cfg.candles), tf, now_ms)
    if len(df) < max(120, cfg.ma_length + 70):
        return [], df
    f = compute_frame(df, cfg)
    cands = base_candidates(f, len(f) - 1, cfg, tick)
    if not cands:
        return [], df

    r = f.iloc[-1]
    candle_open = int(r["time"])
    age_min = (now_ms - (candle_open + TF_MS[tf])) / 60_000
    if age_min > cfg.max_age_factor * TF_MS[tf] / 60_000:
        log.debug("%s %s: señal antigua (%.1f min), se ignora", sym, tf, age_min)
        return [], df

    extras = Extras(api, sym, tf, cfg)
    price, atr = float(r["close"]), float(r["atr"])
    stop_dist = atr * cfg.sl_mult
    by_side: dict[str, dict] = {}

    for kind, side, mode in cands:
        notes: list[str] = []
        stats = {"er": float(r["er"]), "adx": float(r["adx"]), "rvol": float(r["rvol"]) if not pd.isna(r["rvol"]) else None,
                 "body": float(r["bodyRatio"])}
        # 1) timeframe superior
        if cfg.use_htf:
            h = extras.htf_state()
            if h is not None:
                if h.get("n/d"):
                    continue
                if (side == "LONG" and not h["bull"]) or (side == "SHORT" and not h["bear"]):
                    continue
                notes.append(f"HTF {h['tf']}: {'alcista' if side == 'LONG' else 'bajista'} ✓")
        if mode == "cont":
            # 2) delta intrabarra
            if cfg.use_delta:
                d = extras.delta_pct(candle_open, float(r["volume"]))
                if d is None:
                    notes.append("delta n/d")
                else:
                    if (side == "LONG" and d < cfg.delta_min) or (side == "SHORT" and d > -cfg.delta_min):
                        continue
                    stats["delta"] = d
            # 3) nivel del día anterior
            if cfg.use_prev_day:
                pd_ = extras.prev_day()
                if pd_ is None:
                    continue
                if (side == "LONG" and price < pd_[0]) or (side == "SHORT" and price > pd_[1]):
                    continue
                notes.append("nivel día previo ✓")
        slot = by_side.setdefault(side, {"kinds": [], "stats": stats, "notes": notes})
        slot["kinds"].append(kind)

    out = []
    for side, slot in by_side.items():
        sl, tp1, tp2, _ = levels(side, price, stop_dist, cfg)
        out.append(Signal(
            symbol=sym, tf=tf, side=side, kinds=sorted(set(slot["kinds"])), candle_open_ms=candle_open,
            price=price, atr=atr, sl=sl, tp1=tp1, tp2=tp2, stop_dist=stop_dist, prec=prec,
            stats=slot["stats"], notes=slot["notes"],
        ))
    return out, df


# ============================================================
# MENSAJES Y TELEGRAM
# ============================================================
def fmt_price(x: float, prec: int) -> str:
    return f"{x:.{prec}f}"


def build_message(s: Signal, cfg, now_ms: int) -> str:
    long = s.side == "LONG"
    icon = "🟢" if long else "🔴"
    kinds = " + ".join(f"{k} ({KIND_LABEL[k]})" for k in s.kinds)
    base = s.symbol.split("-")[0]
    risk_mult = cfg.long_risk_mult if long else cfg.short_risk_mult
    risk_cash = cfg.account * cfg.risk_pct / 100 * risk_mult
    qty = risk_cash / s.stop_dist if s.stop_dist > 0 else 0.0
    notional = qty * s.price
    capped = ""
    if notional > cfg.account * cfg.max_lev:
        notional = cfg.account * cfg.max_lev
        qty = notional / s.price
        capped = f" (limitado a {cfg.max_lev:g}x)"
    tp2_r = cfg.long_tp2_r if long else cfg.short_tp2_r
    sl_pct = s.stop_dist / s.price * 100
    close_dt = datetime.fromtimestamp((s.candle_open_ms + TF_MS[s.tf]) / 1000, tz=timezone.utc)
    age = max(0, int((now_ms - (s.candle_open_ms + TF_MS[s.tf])) / 60_000))
    st = s.stats
    parts = [f"ER {st['er']:.2f}", f"ADX {st['adx']:.0f}"]
    if st.get("rvol") is not None:
        parts.append(f"RVOL {st['rvol']:.2f}")
    if "delta" in st:
        parts.append(f"Δ {st['delta'] * 100:+.0f}%")
    parts.append(f"cuerpo {st['body']:.2f}")
    tv = f"https://www.tradingview.com/chart/?symbol=BINGX:{s.symbol.replace('-', '')}.P&interval={TV_INTERVAL.get(s.tf, '15')}"
    manage = (f"📋 Gestión: cierra {cfg.partial_pct:g}% en TP1 y mueve el SL a la entrada · "
              f"el resto con trailing {cfg.trail_mult:g}R o TP2")
    if cfg.use_time_stop:
        manage += f" · sal si pasan {cfg.max_bars} velas"
    if cfg.exit_on_opp:
        manage += " o sale señal contraria"
    lines = [
        f"{icon} <b>{s.side}</b> · <b>{html.escape(s.symbol)}</b> · {s.tf}",
        f"<b>{kinds}</b>",
        "",
        f"Precio ref. (cierre vela): <code>{fmt_price(s.price, s.prec)}</code>",
        f"🛑 SL: <code>{fmt_price(s.sl, s.prec)}</code> ({cfg.sl_mult:g} ATR ≈ {sl_pct:.2f}%)",
        f"🎯 TP1: <code>{fmt_price(s.tp1, s.prec)}</code> ({cfg.tp1_r:g}R) · TP2: <code>{fmt_price(s.tp2, s.prec)}</code> ({tp2_r:g}R)",
        "",
        f"Tamaño sugerido (riesgo {cfg.risk_pct * risk_mult:.2f}% de {cfg.account:g} USDT = {risk_cash:.2f} USDT):",
        f"<code>{qty:.6g} {html.escape(base)}</code> ≈ {notional:.2f} USDT nominal{capped}",
        "",
        " · ".join(parts),
    ]
    lines += [n for n in s.notes]
    lines += [
        "",
        manage,
        f"Vela cerrada {close_dt:%H:%M} UTC (hace {age} min)",
        f'<a href="{tv}">Abrir gráfico</a>',
        "",
        "<i>Señal orientativa, no es consejo financiero. Revisa el gráfico antes de entrar.</i>",
    ]
    return "\n".join(lines)


EVENT_TEXT = {
    "SL": ("🛑", "Stop loss"), "TP2": ("🏆", "TP2 alcanzado"), "BE": ("⚪", "Cierre en break-even"),
    "TRAIL": ("✅", "Cierre por trailing"), "TIME": ("⏱", "Cierre por tiempo"),
    "SIGNAL": ("🔁", "Cierre por señal contraria"), "EXPIRED": ("❔", "Sin datos, operación descartada"),
}


def build_event_message(tr: dict, ev: str, cfg) -> str:
    head = f"<b>{tr['side']}</b> · <b>{html.escape(tr['symbol'])}</b> · {tr['tf']} · {'+'.join(tr['kinds'])}"
    if ev == "TP1":
        return (f"🎯 TP1 alcanzado · {head}\n"
                f"Cierra {cfg.partial_pct:g}% y mueve el SL a la entrada <code>{fmt_price(tr['entry'], tr['prec'])}</code>.\n"
                f"El resto sigue con trailing {cfg.trail_mult:g}R hacia TP2 <code>{fmt_price(tr['tp2'], tr['prec'])}</code>.")
    icon, label = EVENT_TEXT.get(ev, ("ℹ️", ev))
    r = tr.get("R", 0.0)
    return (f"{icon} {label} · {head}\n"
            f"Salida ≈ <code>{fmt_price(tr['exit_px'], tr['prec'])}</code> · resultado <b>{r:+.2f}R</b> "
            f"(neto de costes) · {tr['bars']} velas")


def build_stats_message(tracker: Tracker, cfg, days: Optional[float] = None, title: str = "📊 Estadísticas virtuales") -> str:
    since = None if days is None else int((time.time() - days * 86400) * 1000)
    rows = tracker.closed_since(since)
    sm = summarize(rows)
    span = "total" if days is None else f"últimos {days:g} días"
    lines = [f"<b>{title}</b> ({span})", f"Operaciones abiertas ahora: {tracker.open_count()}"]
    if not sm:
        lines.append("Aún no hay operaciones cerradas.")
        return "\n".join(lines)
    lines += [
        f"Cerradas: {sm['n']} · win rate {sm['wr'] * 100:.0f}%",
        f"Expectancia: <b>{sm['avg_r']:+.2f}R</b>/op · total {sm['total_r']:+.1f}R · PF {fmt_pf(sm['pf'])}",
        f"Racha de pérdida máx.: {sm['maxdd']:.1f}R · t-stat {sm['t']:.2f}",
        f"Veredicto: {verdict(sm)}",
    ]
    by_kind = group_by(rows, lambda r: "+".join(r["kinds"]))
    if len(by_kind) > 1:
        lines.append("")
        for k, v in sorted(by_kind.items(), key=lambda kv: -len(kv[1])):
            s2 = summarize(v)
            lines.append(f"{k}: n={s2['n']} · {s2['avg_r']:+.2f}R/op · WR {s2['wr'] * 100:.0f}%")
    lines.append("\n<i>Resultados simulados con tus reglas de salida, sin ejecución real.</i>")
    return "\n".join(lines)


def build_open_message(tracker: Tracker) -> str:
    with tracker.lock:
        opn = list(tracker.open)
    if not opn:
        return "No hay operaciones virtuales abiertas."
    lines = [f"<b>Operaciones abiertas ({len(opn)})</b>"]
    for t in opn:
        stage = "tras TP1 (SL en entrada)" if t["stage"] == 2 else "esperando TP1"
        lines.append(f"{'🟢' if t['side'] == 'LONG' else '🔴'} {html.escape(t['symbol'])} {t['tf']} {'+'.join(t['kinds'])} · "
                     f"ent {fmt_price(t['entry'], t['prec'])} · SL {fmt_price(t['sl'], t['prec'])} · {stage} · {t['bars']} velas")
    return "\n".join(lines)


class Notifier:
    def __init__(self, cfg, dry_run: bool):
        self.cfg, self.dry = cfg, dry_run

    def send(self, text: str) -> bool:
        if self.dry or not (self.cfg.token and self.cfg.chat_id):
            plain = re.sub(r"<[^>]+>", "", text)
            print("\n" + "-" * 50 + "\n" + html.unescape(plain) + "\n" + "-" * 50)
            if not self.dry:
                log.warning("Falta TELEGRAM_TOKEN / TELEGRAM_CHAT_ID: mensaje solo en consola")
            return True
        url = f"https://api.telegram.org/bot{self.cfg.token}/sendMessage"
        payload = {"chat_id": self.cfg.chat_id, "text": text[:4000], "parse_mode": "HTML", "disable_web_page_preview": True}
        for attempt in range(4):
            try:
                r = requests.post(url, json=payload, timeout=15)
                if r.status_code == 429:
                    time.sleep(float(r.json().get("parameters", {}).get("retry_after", 3)) + 1)
                    continue
                if r.ok:
                    return True
                log.error("Telegram %s: %s", r.status_code, r.text[:200])
                return False
            except Exception as ex:  # noqa: BLE001
                log.error("Telegram error: %s", ex)
                time.sleep(1 + attempt)
        return False


class State:
    """Señales ya enviadas (evita duplicados tras reinicios si el fichero persiste)."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.data: dict[str, float] = {}
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                self.data = {}

    def seen(self, key: str) -> bool:
        return key in self.data

    def add(self, key: str) -> None:
        self.data[key] = time.time()

    def save(self) -> None:
        cutoff = time.time() - 3 * 86400
        self.data = {k: v for k, v in self.data.items() if v >= cutoff}
        _atomic_write(self.path, json.dumps(self.data))


# ============================================================
# ESCANEO
# ============================================================
STATUS: dict = {"started": time.time(), "last_scan": {}, "contracts": 0, "last_alert": 0.0}


def select_contracts(contracts: list[dict], cfg, vols: dict[str, float]) -> list[dict]:
    inc = re.compile(cfg.include_re, re.I) if cfg.include_re else None
    exc = re.compile(cfg.exclude_re, re.I) if cfg.exclude_re else None
    out = []
    for c in contracts:
        sym = c["symbol"]
        if inc and not inc.search(sym):
            continue
        if exc and exc.search(sym):
            continue
        if cfg.min_volume > 0 and vols and vols.get(sym, 0.0) < cfg.min_volume:
            continue
        out.append(c)
    return out


def scan(api: BingX, cfg, tf: str, contracts: list[dict], state: State, notifier: Notifier,
         tracker: Optional[Tracker] = None) -> None:
    t0 = time.time()
    now_ms = int(t0 * 1000)
    vols: dict[str, float] = {}
    if cfg.min_volume > 0:
        try:
            vols = api.volumes_24h()
        except Exception as ex:  # noqa: BLE001
            log.warning("No se pudo leer el volumen 24h (%s): se escanea todo", ex)
    syms = select_contracts(contracts, cfg, vols)
    err_lock, errors = threading.Lock(), [0]

    def work(c):
        try:
            sigs, df = analyze_symbol(api, c, tf, cfg, now_ms)
            msgs = tracker.update(c["symbol"], tf, df, sigs) if tracker else []
            return sigs, msgs
        except Exception as ex:  # noqa: BLE001
            with err_lock:
                errors[0] += 1
            log.debug("%s: %s", c["symbol"], ex)
            return [], []

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        results = list(ex.map(work, syms))

    # 1) avisos de las operaciones que ya estaban abiertas (TP1, SL, TP2...)
    for _, msgs in results:
        for m in msgs:
            notifier.send(m)
    if tracker:
        for m in tracker.expire_stale(now_ms):
            notifier.send(m)

    # 2) señales nuevas
    signals = [s for sigs, _ in results for s in sigs]
    new = [s for s in signals if not state.seen(s.key)]
    if tracker:
        new = [s for s in new if not tracker.has_open(s.symbol, s.tf)]
    new.sort(key=lambda s: s.stats.get("er", 0), reverse=True)
    dur = time.time() - t0
    log.info("[%s] %d símbolos en %.0fs · errores %d · señales %d (nuevas %d)", tf, len(syms), dur, errors[0], len(signals), len(new))
    STATUS["last_scan"][tf] = time.time()
    if dur > 0.8 * TF_MS[tf] / 1000:
        log.warning("[%s] el escaneo tarda %.0fs (>80%% de la vela): reduce contratos o sube RATE_LIMIT_RPS", tf, dur)
    if syms and errors[0] / len(syms) > 0.3 and time.time() - STATUS["last_alert"] > 3600:
        STATUS["last_alert"] = time.time()
        notifier.send(f"⚠️ [{tf}] {errors[0]} de {len(syms)} contratos fallaron al descargarse (¿límite de BingX o caída?).")

    limit = cfg.max_per_scan
    if tracker and cfg.max_open > 0:
        limit = min(limit, max(0, cfg.max_open - tracker.open_count()))
    sent, rest = new[:limit], new[limit:]
    for s in sent:
        if notifier.send(build_message(s, cfg, int(time.time() * 1000))):
            state.add(s.key)
            if tracker:
                tracker.add(s)
    if rest:
        names = ", ".join(f"{s.symbol} {s.side} {'+'.join(s.kinds)}" for s in rest)
        notifier.send(f"➕ {len(rest)} señales más ({tf}) omitidas por el límite de señales/posiciones abiertas: {names}")
        for s in rest:
            state.add(s.key)
    state.save()
    if tracker:
        tracker.save()


def command_loop(cfg, notifier: Notifier, tracker: Tracker, stop: threading.Event) -> None:
    """Responde a /stats /semana /abiertas /estado /ayuda (solo desde tu chat)."""
    url = f"https://api.telegram.org/bot{cfg.token}/getUpdates"
    offset = None
    while not stop.is_set():
        try:
            params = {"timeout": 25}
            if offset:
                params["offset"] = offset
            r = requests.get(url, params=params, timeout=40)
            if r.status_code == 409:
                log.warning("Otro proceso está leyendo comandos del bot (409). Reintento en 60s.")
                stop.wait(60)
                continue
            j = r.json()
            for u in j.get("result", []):
                offset = u["update_id"] + 1
                m = u.get("message") or {}
                if str(m.get("chat", {}).get("id")) != str(cfg.chat_id):
                    continue
                text = (m.get("text") or "").strip()
                if not text.startswith("/"):
                    continue
                cmd = text.split()[0].split("@")[0].lower()
                if cmd in ("/stats", "/estadisticas"):
                    notifier.send(build_stats_message(tracker, cfg))
                elif cmd in ("/semana", "/week"):
                    notifier.send(build_stats_message(tracker, cfg, days=7))
                elif cmd in ("/abiertas", "/open"):
                    notifier.send(build_open_message(tracker))
                elif cmd in ("/estado", "/status"):
                    ls = ", ".join(f"{k}: hace {int((time.time() - v) / 60)} min" for k, v in STATUS["last_scan"].items()) or "aún sin escanear"
                    up = (time.time() - STATUS["started"]) / 3600
                    notifier.send(f"✅ Activo hace {up:.1f} h · {STATUS['contracts']} contratos · último escaneo → {ls}")
                elif cmd in ("/ayuda", "/help", "/start"):
                    notifier.send("Comandos: /stats (todo) · /semana (7 días) · /abiertas · /estado")
        except Exception as ex:  # noqa: BLE001
            log.debug("command_loop: %s", ex)
            stop.wait(5)


def run_loop(api: BingX, cfg, notifier: Notifier, state: State, tracker: Optional[Tracker]) -> None:
    tfs = sorted(cfg.timeframes, key=lambda t: TF_MS[t])
    contracts, last_refresh = api.contracts(cfg.quotes), time.time()
    STATUS["contracts"] = len(contracts)
    extra = f" · seguimiento activo ({tracker.open_count()} abiertas)" if tracker else ""
    notifier.send(f"✅ Escáner Granville v2 iniciado · {len(contracts)} contratos · TF {', '.join(tfs)}{extra}")
    stop = threading.Event()
    if tracker and cfg.commands and cfg.token and cfg.chat_id:
        threading.Thread(target=command_loop, args=(cfg, notifier, tracker, stop), daemon=True).start()
    # al arrancar no reescanea la vela que ya cerró (evita duplicados tras un redeploy)
    now0 = time.time()
    last_scanned = {tf: 0 if cfg.scan_on_start else (int(now0 * 1000) // TF_MS[tf]) * TF_MS[tf] - TF_MS[tf] for tf in tfs}
    last_summary_day = None
    while True:
        try:
            if time.time() - last_refresh > 6 * 3600:
                contracts, last_refresh = api.contracts(cfg.quotes), time.time()
                STATUS["contracts"] = len(contracts)
            now = time.time()
            for tf in tfs:
                ms = TF_MS[tf]
                last_open = (int(now * 1000) // ms) * ms - ms
                if last_open > last_scanned[tf] and now * 1000 >= last_open + ms + cfg.delay * 1000:
                    last_scanned[tf] = last_open
                    scan(api, cfg, tf, contracts, state, notifier, tracker)
            utc = datetime.now(timezone.utc)
            if tracker and 0 <= cfg.summary_hour <= 23 and utc.hour == cfg.summary_hour and last_summary_day != utc.date():
                last_summary_day = utc.date()
                notifier.send(build_stats_message(tracker, cfg, days=1, title="🗓 Resumen diario")
                              + "\n\n" + build_stats_message(tracker, cfg, title="Acumulado"))
            time.sleep(2)
        except KeyboardInterrupt:
            log.info("Detenido")
            stop.set()
            if tracker:
                tracker.save()
            state.save()
            return
        except Exception as ex:  # noqa: BLE001
            log.exception("Error en el bucle principal: %s", ex)
            time.sleep(15)


# ============================================================
# BACKTEST (misma lógica de señales y de gestión que el bot en vivo)
# Limitaciones: no aplica el filtro de delta intrabarra (necesitaría 1m histórico), no modela el
# límite de posiciones simultáneas y solo incluye contratos que existen hoy (sesgo de supervivencia).
# ============================================================
def fetch_history(api: BingX, symbol: str, interval: str, days: float) -> pd.DataFrame:
    need = int(days * 86_400_000 // TF_MS[interval]) + 400
    chunks, total, end = [], 0, None
    for _ in range(80):
        df = api.klines(symbol, interval, 1000, end)
        if df.empty:
            break
        chunks.append(df)
        total += len(df)
        if total >= need:
            break
        new_end = int(df["time"].iloc[0]) - 1
        if (end is not None and new_end >= end) or len(df) < 50:
            break
        end = new_end
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks).sort_values("time").drop_duplicates("time").reset_index(drop=True)
    return closed_only(out, interval).tail(need).reset_index(drop=True)


def _align_flags(df: pd.DataFrame, tf: str, bucket_ms: int, cols: dict[str, pd.Series], avail: pd.Series) -> pd.DataFrame:
    """Alinea indicadores de velas agregadas con cada vela base, usando solo velas agregadas ya cerradas."""
    right = pd.DataFrame({"avail": avail.values, **{k: v.values for k, v in cols.items()}}).sort_values("avail")
    left = pd.DataFrame({"t_close": (df["time"] + TF_MS[tf]).values})
    return pd.merge_asof(left, right, left_on="t_close", right_on="avail", direction="backward")


def htf_flags(df: pd.DataFrame, tf: str, htf: str, cfg) -> tuple[np.ndarray, np.ndarray]:
    hms, per = TF_MS[htf], TF_MS[htf] // TF_MS[tf]
    g = df.groupby(df["time"] // hms)
    agg = pd.DataFrame({"time": g["time"].first(), "close": g["close"].last(), "n": g["time"].count()})
    agg = agg[agg["n"] == per].reset_index(drop=True)
    e = ema(agg["close"], cfg.htf_len)
    up = (e > e.shift(1)) if cfg.htf_slope_req else pd.Series(True, index=agg.index)
    dn = (e < e.shift(1)) if cfg.htf_slope_req else pd.Series(True, index=agg.index)
    m = _align_flags(df, tf, hms, {"bull": (agg["close"] > e) & up & (agg.index >= cfg.htf_len),
                                   "bear": (agg["close"] < e) & dn & (agg.index >= cfg.htf_len)}, agg["time"] + hms)
    return m["bull"].fillna(False).astype(bool).to_numpy(), m["bear"].fillna(False).astype(bool).to_numpy()


def prevday_levels(df: pd.DataFrame, tf: str) -> tuple[np.ndarray, np.ndarray]:
    dms, per = 86_400_000, 86_400_000 // TF_MS[tf]
    g = df.groupby(df["time"] // dms)
    agg = pd.DataFrame({"time": g["time"].first(), "high": g["high"].max(), "low": g["low"].min(), "n": g["time"].count()})
    agg = agg[agg["n"] == per].reset_index(drop=True)
    agg["time"] = (agg["time"] // dms) * dms
    m = _align_flags(df, tf, dms, {"pdh": agg["high"], "pdl": agg["low"]}, agg["time"] + dms)
    return m["pdh"].to_numpy(dtype=float), m["pdl"].to_numpy(dtype=float)


def backtest_symbol(df: pd.DataFrame, sym: str, tf: str, prec: int, cfg) -> list[dict]:
    if len(df) < max(120, cfg.ma_length + 70):
        return []
    f = compute_frame(df, cfg)
    tick, n, tfms = 10 ** (-prec), len(f), TF_MS[tf]
    htf = cfg.htf_map.get(tf)
    bull = bear = None
    if cfg.use_htf and htf in TF_MS and TF_MS[htf] > tfms and TF_MS[htf] % tfms == 0:
        bull, bear = htf_flags(df, tf, htf, cfg)
    pdh = pdl = None
    if cfg.use_prev_day:
        pdh, pdl = prevday_levels(df, tf)
    cols = ["buyCross", "buyBounce", "buyExt", "sellCross", "sellBounce", "sellExt"]
    anyf = f[cols].any(axis=1).to_numpy()
    o, h, l, c, t = (df[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "time"))
    atr = f["atr"].to_numpy(dtype=float)
    out, cur = [], None
    for i in range(max(cfg.ma_length + 60, 60), n):
        if cur is not None:
            step_trade(cur, {"time": t[i], "open": o[i], "high": h[i], "low": l[i], "close": c[i]}, cfg)
            if cur["closed"]:
                out.append(cur)
                cur = None
        if not anyf[i]:
            continue
        keep = []
        for kind, side, mode in base_candidates(f, i, cfg, tick):
            if bull is not None and ((side == "LONG" and not bull[i]) or (side == "SHORT" and not bear[i])):
                continue
            if mode == "cont" and pdh is not None:
                if side == "LONG" and (np.isnan(pdh[i]) or c[i] < pdh[i]):
                    continue
                if side == "SHORT" and (np.isnan(pdl[i]) or c[i] > pdl[i]):
                    continue
            keep.append((kind, side))
        if not keep:
            continue
        if cur is not None and cfg.exit_on_opp and any(sd != cur["side"] for _, sd in keep):
            close_at(cur, float(c[i]), "SIGNAL", int(t[i]) + tfms)
            out.append(cur)
            cur = None
        if cur is None:
            sides = {sd for _, sd in keep}
            if len(sides) != 1:
                continue
            side = sides.pop()
            cur = make_trade(sym, tf, side, sorted({k for k, sd in keep if sd == side}), int(t[i]), float(c[i]),
                             float(atr[i]) * cfg.sl_mult, prec, cfg)
    return out


def print_group(title: str, groups: dict[str, list[dict]]) -> None:
    print(f"\n{title}")
    print(f"  {'':16s} {'n':>5s} {'WR':>5s} {'R/op':>7s} {'PF':>5s} {'total R':>8s} {'t':>5s}  veredicto")
    for k, rows in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        s = summarize(rows)
        print(f"  {k:16s} {s['n']:5d} {s['wr'] * 100:4.0f}% {s['avg_r']:+7.2f} {fmt_pf(s['pf']):>5s} {s['total_r']:+8.1f} {s['t']:5.1f}  {verdict(s)}")


def cmd_backtest(api: BingX, cfg, spec: str, days: float, tfs: list[str], csv_path: Optional[str]) -> None:
    contracts = {c["symbol"]: c for c in api.contracts(cfg.quotes)}
    if spec.lower().startswith("top:"):
        top_n = int(spec.split(":")[1])
        vols = api.volumes_24h()
        symbols = [s for s, _ in sorted(vols.items(), key=lambda kv: -kv[1]) if s in contracts][:top_n]
    else:
        symbols = [s.strip().upper() for s in spec.split(",") if s.strip()]
    print(f"Backtest {len(symbols)} contratos · {days:g} días · TF {', '.join(tfs)} · "
          f"SL {cfg.sl_mult:g}ATR · TP1 {cfg.tp1_r:g}R · TP2 L{cfg.long_tp2_r:g}R/S{cfg.short_tp2_r:g}R · señales {','.join(cfg.signal_types)}")
    jobs = [(s, tf) for tf in tfs for s in symbols]
    done = [0]

    def work(job):
        sym, tf = job
        try:
            prec = int(contracts.get(sym, {}).get("pricePrecision", 6) or 6)
            df = fetch_history(api, sym, tf, days)
            return backtest_symbol(df, sym, tf, prec, cfg)
        except Exception as ex:  # noqa: BLE001
            log.warning("%s %s: %s", sym, tf, ex)
            return []
        finally:
            done[0] += 1
            if done[0] % 10 == 0:
                print(f"  ... {done[0]}/{len(jobs)}", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        rows = [r for res in ex.map(work, jobs) for r in res]
    # solo operaciones dentro de la ventana pedida
    cutoff = int((time.time() - days * 86400) * 1000)
    rows = [r for r in rows if r["sig_ms"] >= cutoff]
    sm = summarize(rows)
    if not sm:
        print("Sin operaciones en el periodo.")
        return
    print(f"\n===== RESULTADO GLOBAL =====\nOperaciones: {sm['n']} · win rate {sm['wr'] * 100:.1f}% · expectancia {sm['avg_r']:+.3f}R/op "
          f"· total {sm['total_r']:+.1f}R · PF {fmt_pf(sm['pf'])}\n"
          f"Ganancia media {sm['avg_win']:+.2f}R · pérdida media {sm['avg_loss']:+.2f}R · racha pérdida máx. {sm['maxdd']:.1f}R "
          f"· t-stat {sm['t']:.2f}\nVeredicto: {verdict(sm)}")
    print_group("Por tipo de señal", group_by(rows, lambda r: "+".join(r["kinds"])))
    print_group("Por lado", group_by(rows, lambda r: r["side"]))
    print_group("Por timeframe", group_by(rows, lambda r: r["tf"]))
    print_group("Por motivo de salida", group_by(rows, lambda r: r["reason"]))
    per_sym = group_by(rows, lambda r: r["symbol"])
    ranked = sorted(per_sym.items(), key=lambda kv: summarize(kv[1])["total_r"])
    if len(ranked) > 6:
        print_group("Peores 3 contratos", dict(ranked[:3]))
        print_group("Mejores 3 contratos", dict(ranked[-3:]))
    print("\nLectura: si un tipo de señal sale con t < 0 o 'sin evidencia' y n >= 30, quítalo de SIGNAL_TYPES.\n"
          "Con n < 30 no saques conclusiones. Los costes (comisión+slippage) ya están restados.")
    if csv_path:
        pd.DataFrame([{k: r[k] for k in ("symbol", "tf", "side", "kinds", "sig_ms", "entry", "exit_px", "reason", "bars", "R")}
                      for r in rows]).to_csv(csv_path, index=False)
        print(f"Operaciones guardadas en {csv_path}")


# ============================================================
# HERRAMIENTAS DE DIAGNÓSTICO
# ============================================================
def cmd_history(api: BingX, cfg, symbol: str, tf: str, bars: int) -> None:
    contract = next((c for c in api.contracts(cfg.quotes) if c["symbol"] == symbol), {"symbol": symbol, "pricePrecision": 6})
    tick = 10 ** (-int(contract.get("pricePrecision", 6) or 6))
    df = closed_only(api.klines(symbol, tf, min(1000, max(cfg.candles, bars + 150))), tf)
    f = compute_frame(df, cfg)
    print(f"{symbol} {tf}: señales con filtros base (sin HTF/delta/día previo) en las últimas {bars} velas")
    n = 0
    for i in range(max(0, len(f) - bars), len(f)):
        for kind, side, _ in base_candidates(f, i, cfg, tick):
            t = datetime.fromtimestamp(int(f.iloc[i]["time"]) / 1000, tz=timezone.utc)
            print(f"  {t:%Y-%m-%d %H:%M} UTC  {kind}  {side}  cierre={f.iloc[i]['close']}")
            n += 1
    print(f"Total: {n}")


def cmd_debug(api: BingX, cfg, symbol: str, tf: str) -> None:
    df = closed_only(api.klines(symbol, tf, cfg.candles), tf)
    f = compute_frame(df, cfg)
    r = f.iloc[-1]
    t = datetime.fromtimestamp(int(r["time"]) / 1000, tz=timezone.utc)
    print(f"{symbol} {tf} · última vela cerrada {t:%Y-%m-%d %H:%M} UTC")
    for k in ("close", "ma", "atr", "slope", "er", "adx", "rvol", "bodyRatio", "dist",
              "trendOk", "rvolOk", "climax", "volOk", "bodyOk",
              "buyCross", "buyBounce", "buyExt", "sellCross", "sellBounce", "sellExt"):
        print(f"  {k:10s} {r[k]}")


def cmd_chat_id(cfg) -> None:
    if not cfg.token:
        sys.exit("Falta TELEGRAM_TOKEN")
    r = requests.get(f"https://api.telegram.org/bot{cfg.token}/getUpdates", timeout=15).json()
    seen = {}
    for u in r.get("result", []):
        m = u.get("message") or u.get("channel_post") or {}
        ch = m.get("chat")
        if ch:
            seen[ch["id"]] = ch.get("username") or ch.get("title") or ch.get("first_name")
    if not seen:
        print("Sin mensajes. Escribe /start a tu bot en Telegram y repite este comando.")
    for cid, name in seen.items():
        print(f"chat_id={cid}  ({name})")


def _sigterm(*_a) -> None:
    raise KeyboardInterrupt


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(description="Escáner Granville BingX -> Telegram (solo señales)")
    ap.add_argument("--list", action="store_true", help="lista los contratos que se escanearán")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--get-chat-id", action="store_true", help="muestra tu chat_id (tras escribir /start al bot)")
    ap.add_argument("--debug", metavar="SIMBOLO")
    ap.add_argument("--history", metavar="SIMBOLO")
    ap.add_argument("--backtest", metavar="LISTA", help="símbolos separados por coma o top:N (p. ej. top:30)")
    ap.add_argument("--days", type=float, default=60, help="días de historial para --backtest")
    ap.add_argument("--csv", metavar="FICHERO", help="guarda las operaciones del backtest en CSV")
    ap.add_argument("--bars", type=int, default=200)
    ap.add_argument("--tf", help="timeframe(s), p. ej. 15m o 15m,5m")
    ap.add_argument("--once", action="store_true", help="un solo escaneo y salir")
    ap.add_argument("--dry-run", action="store_true", help="no envía a Telegram, imprime en consola")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    cfg = make_cfg()
    if a.tf:
        cfg.timeframes = [t.strip() for t in a.tf.split(",") if t.strip()]
    bad = [t for t in cfg.timeframes if t not in TF_MS]
    if bad:
        sys.exit(f"Timeframe no soportado: {bad}. Válidos: {list(TF_MS)}")

    if a.get_chat_id:
        return cmd_chat_id(cfg)
    notifier = Notifier(cfg, a.dry_run)
    if a.test_telegram:
        ok = notifier.send("🔔 Prueba del escáner Granville: Telegram funciona.")
        sys.exit(0 if ok else 1)

    api = BingX(cfg.rps)
    if a.backtest:
        return cmd_backtest(api, cfg, a.backtest, a.days, cfg.timeframes, a.csv)
    if a.list:
        contracts = api.contracts(cfg.quotes)
        vols = api.volumes_24h() if cfg.min_volume > 0 else {}
        sel = select_contracts(contracts, cfg, vols)
        print(f"{len(contracts)} contratos activos · {len(sel)} pasan los filtros de volumen/regex")
        for c in sel:
            print(c["symbol"])
        return
    if a.debug:
        return cmd_debug(api, cfg, a.debug, cfg.timeframes[0])
    if a.history:
        return cmd_history(api, cfg, a.history, cfg.timeframes[0], a.bars)

    state = State(cfg.state_file)
    trades_path = cfg.trades_file or str(Path(cfg.state_file).with_name("trades.json"))
    tracker = Tracker(trades_path, cfg) if cfg.track else None
    signal.signal(signal.SIGTERM, _sigterm)
    if a.once:
        contracts = api.contracts(cfg.quotes)
        for tf in sorted(cfg.timeframes, key=lambda t: TF_MS[t]):
            scan(api, cfg, tf, contracts, state, notifier, tracker)
        return
    run_loop(api, cfg, notifier, state, tracker)


if __name__ == "__main__":
    main()

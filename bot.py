#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Escáner Granville (G1/G3/G4/G5/G7/G8) para BingX Perpetual Futures -> Telegram.

SOLO ENVÍA SEÑALES. No opera, no usa claves de BingX (solo datos públicos de mercado).
La lógica replica la estrategia/indicador de TradingView "Granville Adaptativo v3".

Uso rápido:
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

    def klines(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        data = self.get("/openApi/swap/v3/quote/klines", {"symbol": symbol, "interval": interval, "limit": limit})
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


def analyze_symbol(api: BingX, contract: dict, tf: str, cfg, now_ms: int) -> list[Signal]:
    sym = contract["symbol"]
    prec = int(contract.get("pricePrecision", 6) or 6)
    tick = 10 ** (-prec)
    df = closed_only(api.klines(sym, tf, cfg.candles), tf, now_ms)
    if len(df) < max(120, cfg.ma_length + 70):
        return []
    f = compute_frame(df, cfg)
    cands = base_candidates(f, len(f) - 1, cfg, tick)
    if not cands:
        return []

    r = f.iloc[-1]
    candle_open = int(r["time"])
    age_min = (now_ms - (candle_open + TF_MS[tf])) / 60_000
    if age_min > cfg.max_age_factor * TF_MS[tf] / 60_000:
        log.debug("%s %s: señal antigua (%.1f min), se ignora", sym, tf, age_min)
        return []

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
        long = side == "LONG"
        tp2_r = cfg.long_tp2_r if long else cfg.short_tp2_r
        sgn = 1 if long else -1
        out.append(Signal(
            symbol=sym, tf=tf, side=side, kinds=sorted(set(slot["kinds"])), candle_open_ms=candle_open,
            price=price, atr=atr, sl=price - sgn * stop_dist, tp1=price + sgn * stop_dist * cfg.tp1_r,
            tp2=price + sgn * stop_dist * tp2_r, stop_dist=stop_dist, prec=prec, stats=slot["stats"], notes=slot["notes"],
        ))
    return out


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
        f"Vela cerrada {close_dt:%H:%M} UTC (hace {age} min)",
        f'<a href="{tv}">Abrir gráfico</a>',
        "",
        "<i>Señal orientativa, no es consejo financiero. Revisa el gráfico antes de entrar.</i>",
    ]
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
        payload = {"chat_id": self.cfg.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
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
        self.path.write_text(json.dumps(self.data), encoding="utf-8")


# ============================================================
# ESCANEO
# ============================================================
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


def scan(api: BingX, cfg, tf: str, contracts: list[dict], state: State, notifier: Notifier) -> None:
    t0 = time.time()
    now_ms = int(t0 * 1000)
    vols: dict[str, float] = {}
    if cfg.min_volume > 0:
        try:
            vols = api.volumes_24h()
        except Exception as ex:  # noqa: BLE001
            log.warning("No se pudo leer el volumen 24h (%s): se escanea todo", ex)
    syms = select_contracts(contracts, cfg, vols)
    errors = 0

    def work(c):
        nonlocal errors
        try:
            return analyze_symbol(api, c, tf, cfg, now_ms)
        except Exception as ex:  # noqa: BLE001
            errors += 1
            log.debug("%s: %s", c["symbol"], ex)
            return []

    with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
        results = list(ex.map(work, syms))
    signals = [s for r in results for s in r]
    new = [s for s in signals if not state.seen(s.key)]
    new.sort(key=lambda s: s.stats.get("er", 0), reverse=True)
    log.info("[%s] %d símbolos en %.0fs · errores %d · señales %d (nuevas %d)",
             tf, len(syms), time.time() - t0, errors, len(signals), len(new))

    for s in new[: cfg.max_per_scan]:
        if notifier.send(build_message(s, cfg, int(time.time() * 1000))):
            state.add(s.key)
    if len(new) > cfg.max_per_scan:
        extra = new[cfg.max_per_scan:]
        names = ", ".join(f"{s.symbol} {s.side} {'+'.join(s.kinds)}" for s in extra)
        notifier.send(f"➕ {len(extra)} señales más ({tf}) omitidas por el límite: {names}")
        for s in extra:
            state.add(s.key)
    state.save()


def run_loop(api: BingX, cfg, notifier: Notifier, state: State) -> None:
    tfs = sorted(cfg.timeframes, key=lambda t: TF_MS[t])
    contracts, last_refresh = api.contracts(cfg.quotes), time.time()
    notifier.send(f"✅ Escáner Granville iniciado · {len(contracts)} contratos · TF {', '.join(tfs)}")
    last_scanned = {tf: 0 for tf in tfs}
    while True:
        try:
            if time.time() - last_refresh > 6 * 3600:
                contracts, last_refresh = api.contracts(cfg.quotes), time.time()
            now = time.time()
            for tf in tfs:
                ms = TF_MS[tf]
                last_open = (int(now * 1000) // ms) * ms - ms
                if last_open > last_scanned[tf] and now * 1000 >= last_open + ms + cfg.delay * 1000:
                    scan(api, cfg, tf, contracts, state, notifier)
                    last_scanned[tf] = last_open
            time.sleep(2)
        except KeyboardInterrupt:
            log.info("Detenido por el usuario")
            return
        except Exception as ex:  # noqa: BLE001
            log.exception("Error en el bucle principal: %s", ex)
            time.sleep(15)


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


def main() -> None:
    load_env()
    ap = argparse.ArgumentParser(description="Escáner Granville BingX -> Telegram (solo señales)")
    ap.add_argument("--list", action="store_true", help="lista los contratos que se escanearán")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--get-chat-id", action="store_true", help="muestra tu chat_id (tras escribir /start al bot)")
    ap.add_argument("--debug", metavar="SIMBOLO")
    ap.add_argument("--history", metavar="SIMBOLO")
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
    if a.once:
        contracts = api.contracts(cfg.quotes)
        for tf in sorted(cfg.timeframes, key=lambda t: TF_MS[t]):
            scan(api, cfg, tf, contracts, state, notifier)
        return
    run_loop(api, cfg, notifier, state)


if __name__ == "__main__":
    main()

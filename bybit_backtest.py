from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import time
import argparse
import random

import pandas as pd
import numpy as np

try:
    import ccxt
except ImportError:
    ccxt = None


# ============================================================
# Config (Backtest, aligned to your LIVE bot v3 style)
# ============================================================
@dataclass
class Config:
    # Exchange / market
    exchange: str = "bybit"
    market_type: str = "usdt_perp"  # "spot" / "usdt_perp"

    # ✅ Bybit USDT linear perp CCXT symbol format
    symbols: Tuple[str, ...] = (
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "ADA/USDT:USDT", "DOGE/USDT:USDT", "LINK/USDT:USDT",
        "AVAX/USDT:USDT", "LTC/USDT:USDT", "ATOM/USDT:USDT"
    )

    # Timeframes
    tf_entry: str = "15m"
    tf_trend: str = "1h"

    # Backtest window (prefer start/end; fallback to days)
    backtest_days: int = 5
    backtest_start: Optional[str] = None  # e.g. "2021-01-01 00:00"
    backtest_end: Optional[str] = None     # e.g. "2021-12-31 23:45"
    tz: str = "Asia/Taipei"

    # Warmup
    warmup_days_entry: int = 8
    warmup_days_trend: int = 20

    # Trend
    ema_fast: int = 20
    ema_slow: int = 60
    trend_confirm_bars_1h: int = 3

    # Entry
    ema_pullback: int = 20
    atr_len: int = 14
    rsi_len: int = 14
    rsi_long_min: float = 40.0
    rsi_long_max: float = 55.0
    rsi_short_min: float = 45.0
    rsi_short_max: float = 65.0

    # Risk / positions (aligned to live defaults you posted)
    initial_equity: float = 10_000.0
    leverage: float = 3.0
    leverage_map: Dict[str, float] = field(default_factory=dict)

    risk_per_trade: float = 0.008
    total_risk_cap: float = 0.04
    max_positions: int = 4
    max_same_dir: int = 2
    same_dir_risk_cap: float = 0.05

    # Costs / gate (aligned naming to live)
    fee_rate: float = 0.0006          # per-side
    slippage_rate: float = 0.0002     # per-side
    funding_rate_8h: float = 0.0002   # estimate (optional)
    apply_funding: bool = True
    min_rr_after_cost: float = 1.20

    # Margin gate (simulate)
    free_margin_buffer: float = 0.95

    # Selection & correlation (aligned)
    max_entries_per_bar: int = 2
    corr_lookback_bars: int = 96
    corr_threshold: float = 0.75

    # Score weights (aligned to live idea)
    w_ev: float = 1.00
    w_trend: float = 0.60
    w_setup: float = 0.40
    w_liq: float = 0.30

    # Stops/targets
    atr_stop_mult_long: float = 1.6
    atr_stop_mult_short: float = 1.6
    rr_long: float = 1.6
    rr_short: float = 2.0

    # Exits
    max_hold_bars: int = 24
    exit_on_trend_flip: bool = True

    # Session filter (Taipei time)
    session_start_hh: Optional[int] = 20
    session_end_hh: Optional[int] = 2

    # Circuit breaker / cooldown (aligned to live)
    dd_stop: float = 0.12
    cooldown_minutes: int = 30

    # Pending
    pending_expire_bars: int = 6
    brutal_entry_delay_bars: int = 0  # keep 0 to match your live snippet

    # Same-bar resolution
    same_bar_mode: str = "conservative"  # "conservative" / "optimistic" / "closest"

    # Data fetch
    fetch_limit_per_call: int = 1000
    sleep_sec_between_calls: float = 0.05

    # Optional stress mode (kept, but default OFF here)
    brutal: bool = False
    brutal_seed: int = 7
    brutal_slippage_mult: float = 3.0
    brutal_samebar_force_stop: bool = True
    brutal_tp_fail_prob: float = 0.10
    brutal_funding_sigma_8h: float = 0.00035


CFG = Config()


# ============================================================
# Utils / Indicators
# ============================================================
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr


def atr(df: pd.DataFrame, n: int) -> pd.Series:
    return true_range(df).ewm(alpha=1 / n, adjust=False).mean()


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    den = down.ewm(alpha=1 / n, adjust=False).mean()
    rs = up.ewm(alpha=1 / n, adjust=False).mean() / den.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50.0)


def timeframe_to_ms(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60_000
    raise ValueError(f"Unsupported timeframe: {tf}")


def force_ts_int64(df: pd.DataFrame) -> pd.DataFrame:
    if "ts" not in df.columns:
        raise KeyError("DataFrame missing 'ts' column")
    out = df.copy()
    out["ts"] = pd.to_numeric(out["ts"], errors="coerce")
    out = out.dropna(subset=["ts"]).copy()
    out["ts"] = out["ts"].astype(np.int64)
    out = out.sort_values("ts").drop_duplicates(subset=["ts"]).reset_index(drop=True)
    return out


def to_taipei_hour(ts_ms: int, tz: str) -> int:
    return int(pd.to_datetime(ts_ms, unit="ms", utc=True).tz_convert(tz).hour)


def in_session(ts_ms: int, start_h: Optional[int], end_h: Optional[int], tz: str) -> bool:
    if start_h is None or end_h is None:
        return True
    h = to_taipei_hour(ts_ms, tz)
    if start_h == end_h:
        return True
    if start_h < end_h:
        return (h >= start_h) and (h < end_h)
    return (h >= start_h) or (h < end_h)


# ============================================================
# Exchange / Data
# ============================================================
def make_exchange(cfg: Config):
    if ccxt is None:
        raise RuntimeError("ccxt not installed. Run: pip install ccxt")
    ex = getattr(ccxt, cfg.exchange)({"enableRateLimit": True})
    if cfg.market_type == "usdt_perp":
        ex.options["defaultType"] = "swap"
    elif cfg.market_type == "spot":
        ex.options["defaultType"] = "spot"
    else:
        raise ValueError(f"Unknown market_type: {cfg.market_type}")
    return ex


def fetch_ohlcv_full(
    ex,
    symbol: str,
    timeframe: str,
    since_ms: int,
    until_ms: Optional[int],
    cfg: Config,
) -> pd.DataFrame:
    tf_ms = timeframe_to_ms(timeframe)
    limit = int(cfg.fetch_limit_per_call)

    all_rows: List[List[float]] = []
    cursor = int(since_ms)
    end = int(until_ms) if until_ms is not None else None

    while True:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        if not ohlcv:
            break

        all_rows.extend(ohlcv)
        last_ts = int(ohlcv[-1][0])
        next_cursor = last_ts + tf_ms

        if end is not None and next_cursor >= end:
            break
        if next_cursor <= cursor:
            break
        cursor = next_cursor

        if cfg.sleep_sec_between_calls > 0:
            time.sleep(cfg.sleep_sec_between_calls)
        if len(ohlcv) < limit:
            break

    df = pd.DataFrame(all_rows, columns=["ts", "open", "high", "low", "close", "volume"])
    if df.empty:
        return df

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = force_ts_int64(df)

    if until_ms is not None:
        df = df[df["ts"] < int(until_ms)].reset_index(drop=True)

    return df


# ============================================================
# Features (aligned to your LIVE build_features)
# ============================================================
def align_trend_to_entry(df_entry: pd.DataFrame, df_trend: pd.DataFrame) -> pd.DataFrame:
    entry = force_ts_int64(df_entry).set_index("ts")
    trend = force_ts_int64(df_trend).set_index("ts")
    merged = pd.merge_asof(
        entry.sort_index(),
        trend.sort_index(),
        left_index=True,
        right_index=True,
        direction="backward",
        suffixes=("", "_1h"),
        allow_exact_matches=True,
    )
    merged = merged.reset_index().rename(columns={"index": "ts"})
    return merged


def build_trend_features_1h(df1h: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    t = force_ts_int64(df1h)
    t["ema_fast"] = ema(t["close"], cfg.ema_fast)
    t["ema_slow"] = ema(t["close"], cfg.ema_slow)

    base_up = (t["ema_fast"] > t["ema_slow"]) & (t["ema_fast"].diff() > 0)
    base_dn = (t["ema_fast"] < t["ema_slow"]) & (t["ema_fast"].diff() < 0)

    n = max(1, int(cfg.trend_confirm_bars_1h))
    t["trend_up"] = base_up.rolling(n).sum().fillna(0).astype(int) == n
    t["trend_dn"] = base_dn.rolling(n).sum().fillna(0).astype(int) == n
    return t


def build_features(df15: pd.DataFrame, df1h: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    t = build_trend_features_1h(df1h, cfg)

    e = force_ts_int64(df15)
    e["ema_pb"] = ema(e["close"], cfg.ema_pullback)
    e["atr"] = atr(e, cfg.atr_len)
    e["rsi"] = rsi(e["close"], cfg.rsi_len)

    rng = (e["high"] - e["low"]).replace(0, np.nan)
    e["bull_reversal"] = (e["close"] > e["open"]) & ((e["close"] - e["open"]) > 0.3 * rng)
    e["bear_reversal"] = (e["close"] < e["open"]) & ((e["open"] - e["close"]) > 0.3 * rng)
    e["lower_wick"] = (np.minimum(e["open"], e["close"]) - e["low"]) / rng
    e["upper_wick"] = (e["high"] - np.maximum(e["open"], e["close"])) / rng

    merged = align_trend_to_entry(e, t[["ts", "ema_fast", "ema_slow", "trend_up", "trend_dn"]])
    merged["ema_fast_prev"] = merged["ema_fast"].shift(1)

    # Liquidity proxy: rolling log(volume) z-score (simple + stable)
    v = merged["volume"].astype(float).replace(0, np.nan)
    lv = np.log(v)
    m = lv.rolling(96).mean()
    s = lv.rolling(96).std().replace(0, np.nan)
    merged["liq_z"] = ((lv - m) / s).fillna(0.0).clip(-5, 5)

    merged = merged.dropna().reset_index(drop=True)
    return merged


def long_setup_ok(prev_row: pd.Series, cfg: Config) -> bool:
    if not bool(prev_row.get("trend_up", False)):
        return False
    if not in_session(int(prev_row["ts"]), cfg.session_start_hh, cfg.session_end_hh, cfg.tz):
        return False
    near_ema = (prev_row["low"] <= prev_row["ema_pb"] * 1.002) and (prev_row["close"] >= prev_row["ema_pb"] * 0.995)
    if not near_ema:
        return False
    if not (cfg.rsi_long_min <= float(prev_row["rsi"]) <= cfg.rsi_long_max):
        return False
    strong = (float(prev_row["lower_wick"]) >= 0.35) or bool(prev_row["bull_reversal"])
    return bool(strong)


def short_setup_ok(prev_row: pd.Series, cfg: Config) -> bool:
    if not bool(prev_row.get("trend_dn", False)):
        return False
    if not in_session(int(prev_row["ts"]), cfg.session_start_hh, cfg.session_end_hh, cfg.tz):
        return False
    rejected = (prev_row["high"] >= prev_row["ema_pb"] * 0.998) and (prev_row["close"] <= prev_row["ema_pb"] * 1.002)
    rsi_ok = (cfg.rsi_short_min <= float(prev_row["rsi"]) <= cfg.rsi_short_max)
    wick_or_bear = (float(prev_row["upper_wick"]) >= 0.35) or bool(prev_row["bear_reversal"])
    return bool(rejected and rsi_ok and wick_or_bear)


# ============================================================
# Trading objects
# ============================================================
@dataclass
class PendingPlan:
    symbol: str
    side: str  # "LONG"/"SHORT"
    setup_ts: int
    setup_idx: int
    trigger: float
    stop: float
    tp: float
    expire_ts: int
    armed_ts: Optional[int] = None


@dataclass
class Trade:
    symbol: str
    side: str
    entry_ts: int
    entry_idx: int
    entry_px: float
    size: float
    stop: float
    tp: float
    lev: float
    exit_ts: Optional[int] = None
    exit_idx: Optional[int] = None
    exit_px: Optional[float] = None
    reason: str = ""
    funding_paid: float = 0.0
    fees_paid: float = 0.0


# ============================================================
# Risk / Cost helpers (aligned naming to LIVE)
# ============================================================
def get_leverage(cfg: Config, sym: str) -> float:
    lev = cfg.leverage_map.get(sym, cfg.leverage)
    try:
        lev = float(lev)
    except Exception:
        lev = float(cfg.leverage)
    return max(1.0, lev)


def est_roundtrip_cost_frac(cfg: Config) -> float:
    # entry+exit costs (fee+slippage), consistent with live
    slip = float(cfg.slippage_rate) * (float(cfg.brutal_slippage_mult) if cfg.brutal else 1.0)
    fee = float(cfg.fee_rate)
    return 2 * fee + 2 * slip


def hard_filter_rr_after_cost(cfg: Config, entry: float, stop: float, tp: float) -> bool:
    risk = abs(entry - stop)
    reward = abs(tp - entry)
    if risk <= 0 or reward <= 0:
        return False
    cost = entry * est_roundtrip_cost_frac(cfg)
    rr = max(0.0, (reward - cost)) / max(1e-9, (risk + cost))
    return rr >= cfg.min_rr_after_cost


def per_trade_risk(entry_px: float, stop_px: float, size: float) -> float:
    return abs(entry_px - stop_px) * abs(size)


def notional(entry_px: float, size: float) -> float:
    return abs(entry_px * size)


def margin_required(entry_px: float, size: float, lev: float) -> float:
    return notional(entry_px, size) / max(1e-12, lev)


def calc_position_size_by_risk(equity: float, entry_px: float, stop_px: float, risk_frac: float) -> float:
    risk_amount = float(equity) * float(risk_frac)
    per_unit = abs(entry_px - stop_px)
    if per_unit <= 0:
        return 0.0
    return max(0.0, risk_amount / per_unit)


def resolve_same_bar(open_px: float, stop: float, tp: float, cfg: Config) -> str:
    if cfg.brutal and cfg.brutal_samebar_force_stop:
        return "STOP"
    mode = (cfg.same_bar_mode or "conservative").lower()
    if mode == "optimistic":
        return "TP"
    if mode == "conservative":
        return "STOP"
    d_stop = abs(open_px - stop)
    d_tp = abs(tp - open_px)
    return "STOP" if d_stop <= d_tp else "TP"


def apply_fill(px: float, side: str, cfg: Config) -> float:
    slip = float(cfg.slippage_rate) * (float(cfg.brutal_slippage_mult) if cfg.brutal else 1.0)
    return px * (1 + slip) if side.upper() == "LONG" else px * (1 - slip)


def fee_cost(notional_usdt: float, cfg: Config) -> float:
    # per-side fee; we'll charge on entry and exit
    return abs(notional_usdt) * float(cfg.fee_rate)


def funding_cost_8h(notional_usdt: float, cfg: Config) -> float:
    if not cfg.apply_funding:
        return 0.0
    if not cfg.brutal:
        return abs(notional_usdt) * float(cfg.funding_rate_8h)

    # brutal: add noise
    rnd = np.random.normal(0.0, float(cfg.brutal_funding_sigma_8h))
    rate = float(cfg.funding_rate_8h) + rnd
    return abs(notional_usdt) * rate


# ============================================================
# LIVE-like scoring / selection helpers
# ============================================================
def score_plan(cfg: Config, plan: PendingPlan, feats_row: pd.Series) -> float:
    """Same idea as LIVE: (EV proxy / RR after costs) + trend quality + setup quality + liquidity"""
    entry = float(plan.trigger)
    stop = float(plan.stop)
    tp = float(plan.tp)
    risk = abs(entry - stop)
    reward = abs(tp - entry)
    if risk <= 0 or reward <= 0:
        return -1e18

    cost = entry * est_roundtrip_cost_frac(cfg)
    ev = (reward - cost) / max(1e-9, (risk + cost))

    atrv = float(feats_row.get("atr", 0.0) or 0.0)
    ef = float(feats_row.get("ema_fast", 0.0) or 0.0)
    es = float(feats_row.get("ema_slow", 0.0) or 0.0)
    efp = float(feats_row.get("ema_fast_prev", ef) or ef)
    atr_n = max(1e-9, atrv)

    ema_gap_n = abs(ef - es) / atr_n
    ema_slope_n = (ef - efp) / atr_n
    trend_q = ema_gap_n + 0.5 * ema_slope_n

    lw = float(feats_row.get("lower_wick", 0.0) or 0.0)
    uw = float(feats_row.get("upper_wick", 0.0) or 0.0)
    bull = 1.0 if bool(feats_row.get("bull_reversal", False)) else 0.0
    bear = 1.0 if bool(feats_row.get("bear_reversal", False)) else 0.0

    if plan.side.upper() == "LONG":
        setup_q = lw + 0.5 * bull - 0.2 * uw
    else:
        setup_q = uw + 0.5 * bear - 0.2 * lw

    liq = float(feats_row.get("liq_z", 0.0) or 0.0)

    return cfg.w_ev * ev + cfg.w_trend * trend_q + cfg.w_setup * setup_q + cfg.w_liq * liq


def build_return_series(df15: pd.DataFrame, lookback: int) -> np.ndarray:
    c = df15["close"].astype(float).values
    c = c[-(lookback + 1):]
    if len(c) < 16:
        return np.array([])
    r = np.diff(np.log(c))
    return r


def corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 16 or b.size < 16:
        return 0.0
    n = min(a.size, b.size)
    a = a[-n:]
    b = b[-n:]
    if np.std(a) < 1e-12 or np.std(b) < 1e-12:
        return 0.0
    return float(np.corrcoef(a, b)[0, 1])


# ============================================================
# Strategy: create pending plan from prev bar (same as LIVE)
# ============================================================
def make_pending_from_prev(cfg: Config, sym: str, prev: pd.Series) -> Optional[PendingPlan]:
    if pd.isna(prev.get("atr", np.nan)) or float(prev.get("atr", 0.0)) <= 0:
        return None

    ts = int(prev["ts"])
    atrv = float(prev["atr"])

    if long_setup_ok(prev, cfg):
        setup_high = float(prev["high"])
        setup_low = float(prev["low"])
        stop = setup_low - cfg.atr_stop_mult_long * atrv
        trigger = setup_high * 1.0005
        tp = trigger + cfg.rr_long * (trigger - stop)
        expire_ts = ts + cfg.pending_expire_bars * timeframe_to_ms(cfg.tf_entry)
        return PendingPlan(symbol=sym, side="LONG", setup_ts=ts, setup_idx=-1, trigger=trigger, stop=stop, tp=tp, expire_ts=expire_ts)

    if short_setup_ok(prev, cfg):
        setup_low = float(prev["low"])
        setup_high = float(prev["high"])
        stop = setup_high + cfg.atr_stop_mult_short * atrv
        trigger = setup_low * 0.9995
        tp = trigger - cfg.rr_short * (stop - trigger)
        expire_ts = ts + cfg.pending_expire_bars * timeframe_to_ms(cfg.tf_entry)
        return PendingPlan(symbol=sym, side="SHORT", setup_ts=ts, setup_idx=-1, trigger=trigger, stop=stop, tp=tp, expire_ts=expire_ts)

    return None


# ============================================================
# Backtest Engine
# ============================================================
def parse_dt_ms(s: Optional[str], tz: str) -> Optional[int]:
    if not s:
        return None
    dt = pd.Timestamp(s, tz=tz)
    return int(dt.tz_convert("UTC").value // 10**6)


def summarize(trades: List[Trade], equity_curve: List[float], cfg: Config) -> Dict[str, Any]:
    if not equity_curve:
        return {}

    start = float(equity_curve[0])
    end = float(equity_curve[-1])
    ret = (end / start - 1.0) if start > 0 else 0.0

    # drawdown
    peak = -1e18
    max_dd = 0.0
    for x in equity_curve:
        peak = max(peak, x)
        dd = (peak - x) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    # trade stats
    closed = [t for t in trades if t.exit_ts is not None and t.exit_px is not None]
    wins = 0
    gross_win = 0.0
    gross_loss = 0.0
    hold_bars = []

    for t in closed:
        pnl = (t.exit_px - t.entry_px) * t.size if t.side == "LONG" else (t.entry_px - t.exit_px) * t.size
        pnl -= t.fees_paid
        pnl -= t.funding_paid
        if pnl >= 0:
            wins += 1
            gross_win += pnl
        else:
            gross_loss += abs(pnl)
        if t.exit_ts is not None:
            hold_bars.append(int((t.exit_ts - t.entry_ts) // timeframe_to_ms(cfg.tf_entry)))

    n = len(closed)
    winrate = (wins / n * 100.0) if n > 0 else 0.0
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    avg_hold = (sum(hold_bars) / len(hold_bars)) if hold_bars else 0.0

    return {
        "start_equity": start,
        "end_equity": end,
        "total_net_pct": ret * 100.0,
        "max_dd_pct": max_dd * 100.0,
        "trades": n,
        "winrate_pct": winrate,
        "pf": pf,
        "avg_hold_bars": avg_hold,
    }


def run_backtest(cfg: Config) -> Tuple[List[Trade], List[float]]:
    ex = make_exchange(cfg)
    ex.load_markets()

    # determine time window
    end_ms = parse_dt_ms(cfg.backtest_end, cfg.tz)
    start_ms = parse_dt_ms(cfg.backtest_start, cfg.tz)

    if start_ms is None or end_ms is None:
        # fallback by days (end=now, start=end-days)
        now_ms = int(pd.Timestamp.now("UTC").value // 10**6)
        end_ms = end_ms or now_ms
        start_ms = start_ms or (end_ms - int(cfg.backtest_days) * 24 * 60 * 60_000)

    # warmups
    warmup_entry_ms = int(cfg.warmup_days_entry) * 24 * 60 * 60_000
    warmup_trend_ms = int(cfg.warmup_days_trend) * 24 * 60 * 60_000

    # load OHLCV for all symbols
    df15_map: Dict[str, pd.DataFrame] = {}
    df1h_map: Dict[str, pd.DataFrame] = {}
    feats_map: Dict[str, pd.DataFrame] = {}

    print(f"[LOAD] start={pd.to_datetime(start_ms, unit='ms', utc=True)} end={pd.to_datetime(end_ms, unit='ms', utc=True)}")
    for sym in cfg.symbols:
        # fetch with warmup buffer
        since15 = start_ms - warmup_entry_ms
        since1h = start_ms - warmup_trend_ms
        df15 = fetch_ohlcv_full(ex, sym, cfg.tf_entry, since15, end_ms, cfg)
        df1h = fetch_ohlcv_full(ex, sym, cfg.tf_trend, since1h, end_ms, cfg)
        if df15.empty or df1h.empty:
            print(f"[WARN] empty data {sym} 15m={len(df15)} 1h={len(df1h)}")
            continue
        df15_map[sym] = df15
        df1h_map[sym] = df1h
        feats_map[sym] = build_features(df15, df1h, cfg)

    if not feats_map:
        raise RuntimeError("No data loaded. Check symbols / exchange / date range.")

    # build master timeline (15m bar timestamps) using first available symbol
    any_sym = next(iter(feats_map.keys()))
    timeline = feats_map[any_sym]["ts"].values.astype(np.int64)

    # only backtest window
    timeline = timeline[(timeline >= start_ms) & (timeline < end_ms)]
    if timeline.size < 10:
        raise RuntimeError("Timeline too short after filtering window.")

    # state
    equity = float(cfg.initial_equity)
    peak_equity = float(equity)
    equity_curve: List[float] = [equity]

    pending: Dict[str, PendingPlan] = {}
    open_trades: Dict[str, Trade] = {}
    last_exit_time: Dict[str, int] = {}

    rng = np.random.default_rng(cfg.brutal_seed)
    np.random.seed(cfg.brutal_seed)
    random.seed(cfg.brutal_seed)

    # helpers
    def total_open_risk() -> float:
        return float(sum(per_trade_risk(t.entry_px, t.stop, t.size) for t in open_trades.values()))

    def same_dir_open_risk(side: str) -> float:
        side = side.upper()
        return float(sum(per_trade_risk(t.entry_px, t.stop, t.size) for t in open_trades.values() if t.side.upper() == side))

    def used_margin() -> float:
        m = 0.0
        for t in open_trades.values():
            m += margin_required(t.entry_px, t.size, t.lev)
        return float(m)

    def free_margin() -> float:
        return float(max(0.0, equity - used_margin()))

    # results
    all_trades: List[Trade] = []

    # main loop at each 15m bar close timestamp
    tf_ms = timeframe_to_ms(cfg.tf_entry)

    for bar_ts in timeline:
        bar_ts = int(bar_ts)

        # DD lock
        peak_equity = max(peak_equity, equity)
        dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0.0
        dd_lock = dd >= float(cfg.dd_stop)

        # 1) manage exits on this bar (STOP/TP/time/trend flip)
        for sym, tr in list(open_trades.items()):
            feats = feats_map.get(sym)
            if feats is None or feats.empty:
                continue
            f = feats[feats["ts"] <= bar_ts]
            if f.empty:
                continue
            row = f.iloc[-1]

            # find this bar's OHLC from 15m data
            df15 = df15_map[sym]
            cur = df15[df15["ts"] == bar_ts]
            if cur.empty:
                continue
            o = float(cur["open"].iloc[0])
            h = float(cur["high"].iloc[0])
            l = float(cur["low"].iloc[0])
            c = float(cur["close"].iloc[0])

            # time exit
            bars_held = int((bar_ts - tr.entry_ts) // tf_ms)
            if bars_held >= int(cfg.max_hold_bars):
                exit_px = apply_fill(o, "SHORT" if tr.side == "LONG" else "LONG", cfg)  # exit at next open approx
                tr.exit_ts = bar_ts
                tr.exit_idx = int(bars_held)
                tr.exit_px = float(exit_px)
                tr.reason = "TIME"
                # costs
                tr.fees_paid += fee_cost(notional(tr.entry_px, tr.size), cfg) + fee_cost(notional(tr.exit_px, tr.size), cfg)
                if cfg.apply_funding:
                    # approximate: charge per 8h chunk
                    chunks = max(0, int((bars_held * 15) // 480))  # 15m bars -> minutes; 8h=480min
                    tr.funding_paid += chunks * funding_cost_8h(notional(tr.entry_px, tr.size), cfg)
                all_trades.append(tr)
                open_trades.pop(sym, None)
                last_exit_time[sym] = bar_ts
                continue

            # trend invalid
            if cfg.exit_on_trend_flip:
                if tr.side == "LONG" and (not bool(row.get("trend_up", True))):
                    exit_px = apply_fill(o, "SHORT", cfg)
                    tr.exit_ts = bar_ts
                    tr.exit_idx = int(bars_held)
                    tr.exit_px = float(exit_px)
                    tr.reason = "TREND_INVALID"
                    tr.fees_paid += fee_cost(notional(tr.entry_px, tr.size), cfg) + fee_cost(notional(tr.exit_px, tr.size), cfg)
                    if cfg.apply_funding:
                        chunks = max(0, int((bars_held * 15) // 480))
                        tr.funding_paid += chunks * funding_cost_8h(notional(tr.entry_px, tr.size), cfg)
                    all_trades.append(tr)
                    open_trades.pop(sym, None)
                    last_exit_time[sym] = bar_ts
                    continue

                if tr.side == "SHORT" and (not bool(row.get("trend_dn", True))):
                    exit_px = apply_fill(o, "LONG", cfg)
                    tr.exit_ts = bar_ts
                    tr.exit_idx = int(bars_held)
                    tr.exit_px = float(exit_px)
                    tr.reason = "TREND_INVALID"
                    tr.fees_paid += fee_cost(notional(tr.entry_px, tr.size), cfg) + fee_cost(notional(tr.exit_px, tr.size), cfg)
                    if cfg.apply_funding:
                        chunks = max(0, int((bars_held * 15) // 480))
                        tr.funding_paid += chunks * funding_cost_8h(notional(tr.entry_px, tr.size), cfg)
                    all_trades.append(tr)
                    open_trades.pop(sym, None)
                    last_exit_time[sym] = bar_ts
                    continue

            # stop/tp check (intrabar)
            hit_stop = (l <= tr.stop <= h) if tr.side == "LONG" else (l <= tr.stop <= h)
            hit_tp = (l <= tr.tp <= h) if tr.side == "LONG" else (l <= tr.tp <= h)

            if hit_stop and hit_tp:
                which = resolve_same_bar(o, tr.stop, tr.tp, cfg)
                if which == "STOP":
                    hit_tp = False
                else:
                    hit_stop = False

            if hit_stop:
                exit_px = apply_fill(tr.stop, "SHORT" if tr.side == "LONG" else "LONG", cfg)
                tr.exit_ts = bar_ts
                tr.exit_px = float(exit_px)
                tr.reason = "STOP"
            elif hit_tp:
                # optional brutal TP failure
                if cfg.brutal and (rng.random() < float(cfg.brutal_tp_fail_prob)):
                    # treat as no fill
                    pass
                else:
                    exit_px = apply_fill(tr.tp, "SHORT" if tr.side == "LONG" else "LONG", cfg)
                    tr.exit_ts = bar_ts
                    tr.exit_px = float(exit_px)
                    tr.reason = "TP"

            if tr.exit_ts is not None:
                bars_held2 = int((tr.exit_ts - tr.entry_ts) // tf_ms)
                tr.exit_idx = int(bars_held2)
                tr.fees_paid += fee_cost(notional(tr.entry_px, tr.size), cfg) + fee_cost(notional(tr.exit_px, tr.size), cfg)
                if cfg.apply_funding:
                    chunks = max(0, int((bars_held2 * 15) // 480))
                    tr.funding_paid += chunks * funding_cost_8h(notional(tr.entry_px, tr.size), cfg)

                # update equity
                pnl = (tr.exit_px - tr.entry_px) * tr.size if tr.side == "LONG" else (tr.entry_px - tr.exit_px) * tr.size
                pnl -= tr.fees_paid
                pnl -= tr.funding_paid
                equity += float(pnl)

                all_trades.append(tr)
                open_trades.pop(sym, None)
                last_exit_time[sym] = bar_ts

        equity_curve.append(float(equity))

        # 2) pending expiry cleanup
        for sym, plan in list(pending.items()):
            if bar_ts > int(plan.expire_ts):
                pending.pop(sym, None)

        # 3) create new pending from prev bar
        for sym in feats_map.keys():
            if sym in open_trades:
                continue
            feats = feats_map[sym]
            f = feats[feats["ts"] <= bar_ts]
            if len(f) < 2:
                continue
            prev = f.iloc[-2]
            plan = make_pending_from_prev(cfg, sym, prev)
            if plan is None:
                continue
            plan.setup_idx = int(len(f) - 2)
            pending[sym] = plan

        # 4) entries: trigger pending -> select -> risk/margin -> enter
        if dd_lock:
            continue

        open_risk = total_open_risk()

        candidates: List[Tuple[str, PendingPlan, float, np.ndarray]] = []
        for sym, plan in list(pending.items()):
            if sym in open_trades:
                pending.pop(sym, None)
                continue
            if len(open_trades) >= int(cfg.max_positions):
                break

            # cooldown
            last_exit = int(last_exit_time.get(sym, 0) or 0)
            if last_exit and (bar_ts - last_exit < int(cfg.cooldown_minutes) * 60_000):
                continue

            # get current bar OHLC
            df15 = df15_map[sym]
            cur = df15[df15["ts"] == bar_ts]
            if cur.empty:
                continue
            # approximate "mark price" trigger with close
            px = float(cur["close"].iloc[0])

            triggered = (px >= plan.trigger) if plan.side.upper() == "LONG" else (px <= plan.trigger)
            if not triggered:
                continue

            if plan.armed_ts is None:
                plan.armed_ts = bar_ts
                pending[sym] = plan

            delay_ms = int(cfg.brutal_entry_delay_bars) * tf_ms
            if delay_ms > 0 and (bar_ts - int(plan.armed_ts)) < delay_ms:
                continue

            if not hard_filter_rr_after_cost(cfg, float(plan.trigger), float(plan.stop), float(plan.tp)):
                continue

            feats = feats_map[sym]
            f = feats[feats["ts"] <= bar_ts]
            if len(f) < 2:
                continue
            prev = f.iloc[-2]
            sc = score_plan(cfg, plan, prev)

            rs = build_return_series(df15, cfg.corr_lookback_bars)
            candidates.append((sym, plan, sc, rs))

        candidates.sort(key=lambda x: x[2], reverse=True)

        selected: List[Tuple[str, PendingPlan]] = []
        selected_rs: List[np.ndarray] = []
        for sym, plan, sc, rs in candidates:
            ok = True
            for rs2 in selected_rs:
                if abs(corr(rs, rs2)) >= float(cfg.corr_threshold):
                    ok = False
                    break
            if not ok:
                continue
            selected.append((sym, plan))
            selected_rs.append(rs)
            if len(selected) >= int(cfg.max_entries_per_bar):
                break

        for sym, plan in selected:
            if sym in open_trades:
                pending.pop(sym, None)
                continue
            if len(open_trades) >= int(cfg.max_positions):
                break

            # direction caps
            if same_dir_open_risk(plan.side) >= equity * float(cfg.same_dir_risk_cap):
                continue
            if sum(1 for t in open_trades.values() if t.side.upper() == plan.side.upper()) >= int(cfg.max_same_dir):
                continue

            # size by risk
            entry_est = float(plan.trigger)
            stop_px = float(plan.stop)
            size = calc_position_size_by_risk(equity, entry_est, stop_px, float(cfg.risk_per_trade))
            if size <= 0:
                pending.pop(sym, None)
                continue

            # total risk cap
            new_risk = abs(entry_est - stop_px) * abs(size)
            if (open_risk + new_risk) > equity * float(cfg.total_risk_cap):
                continue

            lev = get_leverage(cfg, sym)

            # margin gate
            req = margin_required(entry_est, size, lev)
            fm = free_margin()
            if req > fm * float(cfg.free_margin_buffer):
                continue

            # enter at next open approx (bar open)
            df15 = df15_map[sym]
            cur = df15[df15["ts"] == bar_ts]
            if cur.empty:
                continue
            o = float(cur["open"].iloc[0])
            entry_px = apply_fill(o, plan.side, cfg)

            tr = Trade(
                symbol=sym,
                side=plan.side.upper(),
                entry_ts=bar_ts,
                entry_idx=int(plan.setup_idx + 1),
                entry_px=float(entry_px),
                size=float(size),
                stop=float(plan.stop),
                tp=float(plan.tp),
                lev=float(lev),
            )

            # entry fees
            tr.fees_paid += fee_cost(notional(tr.entry_px, tr.size), cfg)

            open_trades[sym] = tr
            open_risk += new_risk
            pending.pop(sym, None)

    return all_trades, equity_curve


# ============================================================
# CLI
# ============================================================
def apply_args_to_cfg(cfg: Config, args: argparse.Namespace) -> Config:
    # date window
    cfg.backtest_start = args.start
    cfg.backtest_end = args.end
    if args.days is not None:
        cfg.backtest_days = int(args.days)

    # basic toggles
    if args.brutal:
        cfg.brutal = True
    if args.session_off:
        cfg.session_start_hh = None
        cfg.session_end_hh = None

    # override a few common knobs
    if args.risk is not None:
        cfg.risk_per_trade = float(args.risk)
    if args.riskcap is not None:
        cfg.total_risk_cap = float(args.riskcap)
    if args.maxpos is not None:
        cfg.max_positions = int(args.maxpos)
    if args.maxsame is not None:
        cfg.max_same_dir = int(args.maxsame)

    return cfg


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=str, default=None, help="e.g. '2021-01-01 00:00' (Taipei time)")
    p.add_argument("--end", type=str, default=None, help="e.g. '2021-12-31 23:45' (Taipei time)")
    p.add_argument("--days", type=int, default=None, help="fallback if start/end not provided")
    p.add_argument("--brutal", action="store_true", help="enable stress mode")
    p.add_argument("--session_off", action="store_true", help="disable session filter")
    p.add_argument("--risk", type=float, default=None)
    p.add_argument("--riskcap", type=float, default=None)
    p.add_argument("--maxpos", type=int, default=None)
    p.add_argument("--maxsame", type=int, default=None)
    p.add_argument("--out", type=str, default="trades.csv")
    args = p.parse_args()

    cfg = apply_args_to_cfg(CFG, args)
    trades, equity_curve = run_backtest(cfg)
    stats = summarize(trades, equity_curve, cfg)

    # print summary
    print("\n===== PORTFOLIO RESULT (LIVE-like Backtest) =====")
    for k, v in stats.items():
        if isinstance(v, float):
            print(f"{k}: {v:.4f}")
        else:
            print(f"{k}: {v}")

    # export trades
    rows = []
    for t in trades:
        if t.exit_ts is None:
            continue
        rows.append({
            "symbol": t.symbol,
            "side": t.side,
            "entry_ts": t.entry_ts,
            "entry_px": t.entry_px,
            "size": t.size,
            "stop": t.stop,
            "tp": t.tp,
            "lev": t.lev,
            "exit_ts": t.exit_ts,
            "exit_px": t.exit_px,
            "reason": t.reason,
            "funding_paid": t.funding_paid,
            "fees_paid": t.fees_paid,
        })
    pd.DataFrame(rows).to_csv(args.out, index=False)
    print(f"\nSaved trades -> {args.out}")


if __name__ == "__main__":
    main()
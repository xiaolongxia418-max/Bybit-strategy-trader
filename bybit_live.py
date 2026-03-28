from __future__ import annotations

# ===== Required imports =====
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple, Any
import os
import time
import json
import traceback
import random  # ✅ FIX: used by make_bot_tag()

import numpy as np
import pandas as pd
import ccxt
import requests

# ===== SECRETS - loaded from environment variables =====
# DO NOT hardcode keys here. Set these in your environment:
#   export BYBIT_API_KEY="your_key"
#   export BYBIT_API_SECRET="your_secret"
#   export TG_BOT_TOKEN="your_telegram_bot_token"
#   export TG_CHAT_ID="your_telegram_chat_id"

import os
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

# ============================================================
# Config (Strategy from your backtest + Engineering from live bot)
# ============================================================
@dataclass
class Config:
    # ✅ FIX: Bybit USDT linear perp CCXT symbol format: "BTC/USDT:USDT"
    symbols: Tuple[str, ...] = (
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "ADA/USDT:USDT", "DOGE/USDT:USDT", "LINK/USDT:USDT",
        "AVAX/USDT:USDT", "LTC/USDT:USDT", "ATOM/USDT:USDT"
    )
    market_type: str = "swap"
    exchange: str = "bybit"
    category: str = "linear"
    one_way_mode: bool = True  # assumes one-way

    # timeframes (same as backtest)
    tf_entry: str = "15m"
    tf_trend: str = "1h"

    # warmup
    warmup_entry_bars: int = 600
    warmup_trend_bars: int = 300

    # trend (same as backtest)
    ema_fast: int = 20
    ema_slow: int = 60
    trend_confirm_bars_1h: int = 3

    # entry (same as backtest)
    ema_pullback: int = 20
    atr_len: int = 14
    rsi_len: int = 14
    rsi_long_min: float = 40.0
    rsi_long_max: float = 55.0
    rsi_short_min: float = 45.0
    rsi_short_max: float = 65.0

    # position/risk (same as backtest; equity-based)
    leverage: float = 3.0
    leverage_map: Dict[str, float] = field(default_factory=dict)
    risk_per_trade: float = 0.008
    total_risk_cap: float = 0.04
    max_positions: int = 4
    max_same_dir: int = 2

    # ===== Costs / Gate =====
    fee_rate: float = 0.0006           # ✅ per-side fee (單邊)
    slippage_rate: float = 0.0002      # ✅ per-side slippage (單邊)
    funding_rate_8h: float = 0.0002    # optional estimate for EV filter (set 0 if unused)
    min_rr_after_cost: float = 1.20    # Hard filter: RR after costs must be >= this

    # ===== Margin gate =====
    free_margin_buffer: float = 0.95   # require required_margin <= free_usdt * buffer

    # ===== Signal selection =====
    max_entries_per_bar: int = 2
    corr_lookback_bars: int = 96       # 96 x 15m = 24h
    corr_threshold: float = 0.75

    # Score weights
    w_ev: float = 1.00
    w_trend: float = 0.60
    w_setup: float = 0.40
    w_liq: float = 0.30
    w_stability: float = 0.00         # reserved

    # ===== Direction risk cap (separate from total_risk_cap) =====
    same_dir_risk_cap: float = 0.05

    # ===== TP on exchange =====
    tp_on_exchange: bool = True

    bot_tag_prefix: str = "LPB3"       # 用於標記/辨識本機器人掛單
    heartbeat_sec: int = 1800          # Telegram 心跳間隔（秒），預設30分鐘

    # pending plan
    pending_expire_bars: int = 6
    brutal_entry_delay_bars: int = 0   # live default 0

    # stops/targets (same as backtest)
    atr_stop_mult_long: float = 1.6
    atr_stop_mult_short: float = 1.6
    rr_long: float = 1.6
    rr_short: float = 2.0

    # exits
    max_hold_bars: int = 24  # in 15m bars => 6h
    exit_on_trend_flip: bool = True

    # session filter (Taipei time) - optional
    session_start_hh: Optional[int] = 20
    session_end_hh: Optional[int] = 2

    # DD circuit breaker (engineering)
    dd_stop: float = 0.30  # 12%

    # cooldown after exit (engineering)
    cooldown_minutes: int = 30

    # polling
    poll_sec: int = 90
    per_symbol_fetch_sleep: float = 0.4
    close_delay_sec: int = 10  # delay after bar close to avoid spike

    # persistence
    state_path: str = "state_live_portfolio.json"

    # misc
    tz: str = "Asia/Taipei"
    fetch_limit_per_call: int = 1000


CFG = Config()


# ============================================================
# Telegram
# ============================================================
def tg_send(msg: str):
    token = (TG_BOT_TOKEN or "").strip()
    chat_id = (TG_CHAT_ID or "").strip()
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": msg[:3900]}, timeout=10)
    except Exception:
        pass


# ============================================================
# Retry / Rate-limit helpers
# ============================================================
def safe_sleep(sec: float):
    time.sleep(max(0.0, float(sec)))


def retry_call(fn, *args, **kwargs):
    delay = 1.0
    for i in range(7):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            is_rate = ("Too many visits" in msg) or ("RateLimit" in msg) or ("10006" in msg)
            is_net = ("NetworkError" in msg) or ("ETIMEDOUT" in msg) or ("RequestTimeout" in msg)
            if is_rate or is_net:
                tg_send(f"⚠️ RETRY: backoff {delay:.1f}s (try {i+1}/7) err={type(e).__name__}: {msg[:120]}")
                safe_sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            raise
    raise RuntimeError("retry_call exceeded retries")


# ============================================================
# Indicators (same as backtest)
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


def to_taipei_hour(ts_ms: int) -> int:
    return int((pd.to_datetime(ts_ms, unit="ms", utc=True).tz_convert(CFG.tz)).hour)


def in_session(ts_ms: int, start_h: Optional[int], end_h: Optional[int]) -> bool:
    if start_h is None or end_h is None:
        return True
    h = to_taipei_hour(ts_ms)
    if start_h == end_h:
        return True
    if start_h < end_h:
        return (h >= start_h) and (h < end_h)
    return (h >= start_h) or (h < end_h)


def timeframe_to_ms(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60_000
    raise ValueError(f"Unsupported timeframe: {tf}")


def force_ts_int64(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ts"] = pd.to_numeric(out["ts"], errors="coerce")
    out = out.dropna(subset=["ts"]).copy()
    out["ts"] = out["ts"].astype(np.int64)
    out = out.sort_values("ts").drop_duplicates(subset=["ts"]).reset_index(drop=True)
    return out


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
class LiveTrade:
    symbol: str
    side: str  # "LONG"/"SHORT"
    qty: float
    entry_px: float
    entry_ts: int
    entry_idx: int
    stop: float
    tp: float
    lev: float
    setup_ts: int
    init_risk_dist: float
    sl_tag: str = ""
    tp_tag: str = ""
    tp_fallback_local: bool = False  # TP掛單失敗時，改用本地TP watcher備援
    last_update_ts: int = 0
    reason: str = ""


# ============================================================
# Exchange (Bybit via CCXT)
# ============================================================
def make_exchange() -> ccxt.Exchange:
    key = (BYBIT_API_KEY or "").strip()
    sec = (BYBIT_API_SECRET or "").strip()
    if not key or not sec:
        raise RuntimeError("Missing BYBIT_API_KEY / BYBIT_API_SECRET in code constants.")

    ex = ccxt.bybit({
        "enableRateLimit": True,
        "apiKey": key,
        "secret": sec,
        "options": {"defaultType": "swap"},
    })
    return ex


def round_amount(ex: ccxt.Exchange, symbol: str, amount: float) -> float:
    try:
        return float(ex.amount_to_precision(symbol, amount))
    except Exception:
        return float(amount)


def round_price(ex: ccxt.Exchange, symbol: str, price: float) -> float:
    try:
        return float(ex.price_to_precision(symbol, price))
    except Exception:
        return float(price)


def set_leverage(ex: ccxt.Exchange, symbol: str, leverage: float):
    lev = max(1.0, float(leverage))
    try:
        if hasattr(ex, "set_leverage"):
            retry_call(ex.set_leverage, lev, symbol)
    except Exception as e:
        tg_send(f"⚠️ set_leverage failed {symbol}: {type(e).__name__}: {e}")


def fetch_equity_usdt(ex: ccxt.Exchange) -> float:
    bal = retry_call(ex.fetch_balance)
    if "USDT" in bal.get("total", {}):
        return float(bal["total"]["USDT"])
    if "USDT" in bal:
        b = bal["USDT"]
        return float(b.get("total") or (b.get("free", 0) + b.get("used", 0)))
    return float(bal.get("total", {}).get("USD", 0.0))


def fetch_free_usdt(ex: ccxt.Exchange) -> float:
    bal = retry_call(ex.fetch_balance)
    if "USDT" in bal:
        b = bal["USDT"]
        if isinstance(b, dict) and "free" in b:
            return float(b.get("free") or 0.0)
    return float(bal.get("free", {}).get("USDT", 0.0))


def est_roundtrip_cost_frac() -> float:
    # rough estimate: entry+exit
    return 2 * CFG.fee_rate + 2 * CFG.slippage_rate


def hard_filter_rr_after_cost(entry: float, stop: float, tp: float) -> bool:
    risk = abs(entry - stop)
    reward = abs(tp - entry)
    if risk <= 0 or reward <= 0:
        return False
    cost = entry * est_roundtrip_cost_frac()
    rr = max(0.0, (reward - cost)) / max(1e-9, (risk + cost))
    return rr >= CFG.min_rr_after_cost


def score_plan(plan: PendingPlan, feats_row: pd.Series) -> float:
    """多訊號排序用的分數：成本後RR(底盤) + 趨勢力度 + 型態品質。
    不依賴 ADX，避免 live 欄位缺失造成排序失真。
    """
    entry = float(plan.trigger)
    stop = float(plan.stop)
    tp = float(plan.tp)
    risk = abs(entry - stop)
    reward = abs(tp - entry)
    if risk <= 0 or reward <= 0:
        return -1e18

    # 底盤：成本後 RR (EV proxy)
    cost = entry * est_roundtrip_cost_frac()
    ev = (reward - cost) / max(1e-9, (risk + cost))

    # 趨勢力度：用 EMA gap / ATR 正規化 + EMA20 slope / ATR
    atrv = float(feats_row.get("atr", 0.0) or 0.0)
    ema_fast = float(feats_row.get("ema_fast", 0.0) or 0.0)
    ema_slow = float(feats_row.get("ema_slow", 0.0) or 0.0)
    ema_fast_prev = float(feats_row.get("ema_fast_prev", ema_fast) or ema_fast)
    atr_n = max(1e-9, atrv)

    ema_gap_n = abs(ema_fast - ema_slow) / atr_n
    ema_slope_n = (ema_fast - ema_fast_prev) / atr_n
    trend_q = ema_gap_n + 0.5 * ema_slope_n

    # 型態品質：wick / reversal
    lw = float(feats_row.get("lower_wick", 0.0) or 0.0)
    uw = float(feats_row.get("upper_wick", 0.0) or 0.0)
    bull = 1.0 if bool(feats_row.get("bull_reversal", False)) else 0.0
    bear = 1.0 if bool(feats_row.get("bear_reversal", False)) else 0.0

    if plan.side.upper() == "LONG":
        setup_q = lw + 0.5 * bull - 0.2 * uw
    else:
        setup_q = uw + 0.5 * bear - 0.2 * lw

    return CFG.w_ev * ev + CFG.w_trend * trend_q + CFG.w_setup * setup_q


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


def fetch_open_orders_symbol(ex: ccxt.Exchange, symbol: str) -> List[Dict[str, Any]]:
    """Try to fetch BOTH normal open orders and conditional/trigger orders.
    CCXT behavior differs by version; we merge results and de-dup by id/orderLinkId.
    """
    seen = set()
    out: List[Dict[str, Any]] = []

    def _add(orders: List[Dict[str, Any]]):
        for o in orders or []:
            oid = o.get("id") or o.get("orderId") or o.get("info", {}).get("orderId")
            link = o.get("clientOrderId") or o.get("info", {}).get("orderLinkId")
            key = oid or link
            if not key:
                key = json.dumps(o.get("info", o), sort_keys=True, default=str)[:200]
            if key in seen:
                continue
            seen.add(key)
            out.append(o)

    # 1) normal open orders
    try:
        _add(retry_call(ex.fetch_open_orders, symbol, None, {"category": CFG.category}))
    except Exception:
        try:
            _add(retry_call(ex.fetch_open_orders, symbol))
        except Exception:
            pass

    # 2) try variants that some ccxt builds use for stop/trigger orders
    for extra in (
        {"category": CFG.category, "stop": True},
        {"category": CFG.category, "trigger": True},
        {"category": CFG.category, "orderType": "Stop"},
        {"stop": True},
    ):
        try:
            _add(retry_call(ex.fetch_open_orders, symbol, None, extra))
        except Exception:
            pass

    return out


def fetch_live_positions(ex: ccxt.Exchange) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    try:
        poss = retry_call(ex.fetch_positions)
    except Exception:
        poss = retry_call(ex.fetch_positions, None, {"category": CFG.category})
    for p in poss:
        sym = p.get("symbol")
        if not sym:
            continue
        contracts = p.get("contracts")
        if contracts is None:
            contracts = p.get("size") or 0.0
        try:
            size = float(contracts or 0.0)
        except Exception:
            size = 0.0
        if abs(size) <= 0:
            continue
        side = str(p.get("side") or "").upper()
        entry = float(p.get("entryPrice") or p.get("average") or 0.0)
        out[sym] = {
            "symbol": sym,
            "side": "LONG" if side == "LONG" else "SHORT",
            "qty": abs(size),
            "entry": entry,
            "raw": p,
        }
    return out


def get_order_client_id(o: Dict[str, Any]) -> str:
    info = o.get("info", {}) or {}
    return str(
        info.get("orderLinkId")
        or info.get("clientOrderId")
        or o.get("clientOrderId")
        or o.get("clientOrderID")
        or ""
    )


def make_bot_tag(symbol: str, kind: str) -> str:
    ts = int(time.time() * 1000)
    rnd = ''.join(random.choice('0123456789abcdef') for _ in range(6))
    sym = symbol.replace("/", "").replace(":", "")
    return f"{CFG.bot_tag_prefix}-{kind}-{sym}-{ts}-{rnd}"


def cancel_bot_orders_symbol(ex: ccxt.Exchange, symbol: str):
    """只取消帶有 bot_tag_prefix 的掛單，避免刪到手動單/其他策略單。"""
    try:
        orders = fetch_open_orders_symbol(ex, symbol)
    except Exception:
        orders = []
    for o in orders:
        cid = get_order_client_id(o)
        if cid.startswith(CFG.bot_tag_prefix + "-"):
            try:
                retry_call(ex.cancel_order, o["id"], symbol, {"category": CFG.category})
            except Exception:
                try:
                    retry_call(ex.cancel_order, o["id"], symbol)
                except Exception:
                    pass


def has_tagged_order(orders: List[Dict[str, Any]], tag: str) -> bool:
    if not tag:
        return False
    for o in orders:
        if get_order_client_id(o) == tag:
            return True
    return False


def place_market_entry(ex: ccxt.Exchange, symbol: str, side: str, qty: float) -> Dict[str, Any]:
    order_side = "buy" if side.upper() == "LONG" else "sell"
    qty = round_amount(ex, symbol, qty)
    if qty <= 0:
        raise ValueError("qty<=0 after rounding")
    params = {"category": CFG.category}
    return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)


def place_reduceonly_market_exit(ex: ccxt.Exchange, symbol: str, side: str, qty: float) -> Dict[str, Any]:
    order_side = "sell" if side.upper() == "LONG" else "buy"
    qty = round_amount(ex, symbol, qty)
    if qty <= 0:
        raise ValueError("qty<=0 after rounding")
    params = {"reduceOnly": True, "category": CFG.category}
    return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)


def _bybit_trigger_direction_int(expected: str) -> int:
    """Bybit v5: triggerDirection 1=rise-to, 2=fall-to."""
    expected = expected.lower().strip()
    if expected in ("rise", "ascending", "up", "above"):
        return 1
    if expected in ("fall", "descending", "down", "below"):
        return 2
    raise ValueError(f"unknown expected direction: {expected}")


def _bybit_position_idx(side: str) -> int:
    # one-way: 0 ; hedge-mode: 1 for long, 2 for short
    if getattr(CFG, "one_way_mode", True):
        return 0
    return 1 if side.upper() == "LONG" else 2


def place_stop_market_reduceonly(
    ex: ccxt.Exchange,
    symbol: str,
    side: str,
    qty: float,
    stop_price: float,
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Place a reduce-only stop-market close order (Bybit linear/inverse).
    Tries Bybit-v5 triggerDirection int first, then CCXT manual-style ascending/descending.
    """
    order_side = "sell" if side.upper() == "LONG" else "buy"
    qty = round_amount(ex, symbol, qty)
    stop_price = float(round_price(ex, symbol, stop_price))
    tag = tag or make_bot_tag(symbol, "SL")

    # LONG SL triggers when price falls to stop; SHORT SL triggers when price rises to stop
    trig_int = _bybit_trigger_direction_int("fall" if side.upper() == "LONG" else "rise")
    trig_str = "descending" if side.upper() == "LONG" else "ascending"

    base = {
        "reduceOnly": True,
        "closeOnTrigger": True,  # ✅ ensure it closes, not opens
        "triggerPrice": stop_price,
        "triggerBy": "MarkPrice",
        "category": CFG.category,
        "orderLinkId": tag,
        "clientOrderId": tag,
        "positionIdx": _bybit_position_idx(side),
    }

    # Try v5-style integer triggerDirection (most reliable on Bybit v5)
    try:
        params = dict(base, triggerDirection=trig_int)
        return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)
    except Exception as e1:
        # Fallback: CCXT manual ascending/descending
        try:
            params = dict(base, triggerDirection=trig_str)
            return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)
        except Exception as e2:
            # Fallback: some builds still accept stopPrice
            try:
                params = dict(base)
                params.pop("triggerPrice", None)
                params["stopPrice"] = stop_price
                params["triggerDirection"] = trig_int
                return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)
            except Exception as e3:
                raise RuntimeError(f"place_stop_market_reduceonly failed A={e1} B={e2} C={e3}")


def place_take_profit_market_reduceonly(
    ex: ccxt.Exchange,
    symbol: str,
    side: str,
    qty: float,
    tp_price: float,
    tag: Optional[str] = None,
) -> Dict[str, Any]:
    """Place a reduce-only take-profit market close order (Bybit linear/inverse)."""
    order_side = "sell" if side.upper() == "LONG" else "buy"
    qty = round_amount(ex, symbol, qty)
    tp_price = float(round_price(ex, symbol, tp_price))
    tag = tag or make_bot_tag(symbol, "TP")

    # LONG TP triggers when price rises to tp; SHORT TP triggers when price falls to tp
    trig_int = _bybit_trigger_direction_int("rise" if side.upper() == "LONG" else "fall")
    trig_str = "ascending" if side.upper() == "LONG" else "descending"

    base = {
        "reduceOnly": True,
        "closeOnTrigger": True,
        "triggerPrice": tp_price,
        "triggerBy": "MarkPrice",
        "category": CFG.category,
        "orderLinkId": tag,
        "clientOrderId": tag,
        "positionIdx": _bybit_position_idx(side),
    }

    try:
        params = dict(base, triggerDirection=trig_int)
        return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)
    except Exception as e1:
        try:
            params = dict(base, triggerDirection=trig_str)
            return retry_call(ex.create_order, symbol, "market", order_side, qty, None, params)
        except Exception as e2:
            raise RuntimeError(f"place_take_profit_market_reduceonly failed A={e1} B={e2}")


def verify_or_fallback_protection(
    ex: ccxt.Exchange,
    tr: LiveTrade,
    want_tp_on_exchange: bool,
    tg_prefix: str = "",
):
    """驗證 SL/TP 是否真的掛在交易所。若 TP 掛單失敗，改用本地 TP watcher 備援並發 TG。"""
    try:
        orders = fetch_open_orders_symbol(ex, tr.symbol)
    except Exception:
        orders = []

    sl_ok = has_tagged_order(orders, tr.sl_tag) if tr.sl_tag else False
    tp_ok = has_tagged_order(orders, tr.tp_tag) if tr.tp_tag else False

    if not sl_ok:
        try:
            tr.sl_tag = tr.sl_tag or make_bot_tag(tr.symbol, "SL")
            place_stop_market_reduceonly(ex, tr.symbol, tr.side, tr.qty, tr.stop, tag=tr.sl_tag)
            orders = fetch_open_orders_symbol(ex, tr.symbol)
            sl_ok = has_tagged_order(orders, tr.sl_tag)
        except Exception as e:
            tg_send(f"🛑 {tg_prefix}SL VERIFY/REPLACE FAIL {tr.symbol} err={e}")

    if want_tp_on_exchange:
        if not tp_ok:
            try:
                tr.tp_tag = tr.tp_tag or make_bot_tag(tr.symbol, "TP")
                place_take_profit_market_reduceonly(ex, tr.symbol, tr.side, tr.qty, tr.tp, tag=tr.tp_tag)
                orders = fetch_open_orders_symbol(ex, tr.symbol)
                tp_ok = has_tagged_order(orders, tr.tp_tag)
            except Exception as e:
                tg_send(f"⚠️ {tg_prefix}TP VERIFY/REPLACE FAIL {tr.symbol} err={e}")

        if not tp_ok:
            tr.tp_fallback_local = True
            tg_send(f"⚠️ {tg_prefix}TP NOT FOUND -> LOCAL TP WATCHER ENABLED {tr.symbol} tp={tr.tp:.6f}")

    return tr


def fetch_mark_price(ex: ccxt.Exchange, symbol: str) -> float:
    """Prefer MarkPrice for triggers/TP watcher to align with SL/TP triggerBy=MarkPrice."""
    t = retry_call(ex.fetch_ticker, symbol)
    for k in ["mark", "index", "last", "close"]:
        v = t.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    raise RuntimeError("No price in ticker")


def fetch_last_price(ex: ccxt.Exchange, symbol: str) -> float:
    """For reporting/UI only."""
    t = retry_call(ex.fetch_ticker, symbol)
    for k in ["last", "close", "mark", "index"]:
        v = t.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    raise RuntimeError("No price in ticker")


# ============================================================
# Data cache (OHLCV)
# ============================================================
def _tf_to_ms(tf: str) -> int:
    tf = str(tf).strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60_000
    if tf.endswith("h"):
        return int(tf[:-1]) * 60 * 60_000
    if tf.endswith("d"):
        return int(tf[:-1]) * 24 * 60 * 60_000
    raise ValueError(f"Unsupported timeframe: {tf}")


class DataCache:
    """OHLCV cache with incremental updates to reduce rate-limit pressure."""

    def __init__(self, ex: ccxt.Exchange):
        self.ex = ex
        self.cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    def _rows_to_df(self, rows: List[List[Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["ts"]).copy()
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna().copy()
        df["ts"] = df["ts"].astype(np.int64)
        df = df.sort_values("ts").drop_duplicates(subset=["ts"]).reset_index(drop=True)
        return df

    def fetch_ohlcv_df(self, symbol: str, tf: str, limit: int, since: Optional[int] = None) -> pd.DataFrame:
        rows = retry_call(self.ex.fetch_ohlcv, symbol, timeframe=tf, since=since, limit=limit)
        return self._rows_to_df(rows)

    def update(self, symbol: str, tf: str, limit: int) -> pd.DataFrame:
        """Update cache for (symbol, tf).

        - If no cache: fetch full warmup `limit`.
        - If cached: incremental fetch since last_ts - 2*tf_ms (to heal partial bars),
          then merge & keep last `limit`.
        """
        key = (symbol, tf)
        prev = self.cache.get(key)
        if prev is None or prev.empty:
            df = self.fetch_ohlcv_df(symbol, tf, limit=limit)
            self.cache[key] = df
            return df

        tf_ms = _tf_to_ms(tf)
        last_ts = int(prev["ts"].iloc[-1])
        since = max(0, last_ts - 2 * tf_ms)

        # fetch only small chunk (cap to avoid huge bursts)
        fetch_limit = min(max(50, int(limit * 0.25)), 200)
        try:
            inc = self.fetch_ohlcv_df(symbol, tf, limit=fetch_limit, since=since)
        except Exception:
            # last resort: small backward fetch without since
            inc = self.fetch_ohlcv_df(symbol, tf, limit=min(limit, 200))

        if inc.empty:
            df = prev.tail(limit).reset_index(drop=True)
            self.cache[key] = df
            return df

        merged = pd.concat([prev, inc], ignore_index=True)
        merged = merged.sort_values("ts").drop_duplicates(subset=["ts"]).reset_index(drop=True)
        df = merged.tail(limit).reset_index(drop=True)
        self.cache[key] = df
        return df

    def get(self, symbol: str, tf: str) -> Optional[pd.DataFrame]:
        return self.cache.get((symbol, tf))


# ============================================================
# Feature building (same as backtest)
# ============================================================
def build_trend_features_1h(df1h: pd.DataFrame) -> pd.DataFrame:
    t = force_ts_int64(df1h)
    t["ema_fast"] = ema(t["close"], CFG.ema_fast)
    t["ema_slow"] = ema(t["close"], CFG.ema_slow)
    base_up = (t["ema_fast"] > t["ema_slow"]) & (t["ema_fast"].diff() > 0)
    base_dn = (t["ema_fast"] < t["ema_slow"]) & (t["ema_fast"].diff() < 0)
    n = max(1, int(CFG.trend_confirm_bars_1h))
    t["trend_up"] = base_up.rolling(n).sum().fillna(0).astype(int) == n
    t["trend_dn"] = base_dn.rolling(n).sum().fillna(0).astype(int) == n
    return t


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


def build_features(df15: pd.DataFrame, df1h: pd.DataFrame) -> pd.DataFrame:
    t = build_trend_features_1h(df1h)
    e = force_ts_int64(df15)
    e["ema_pb"] = ema(e["close"], CFG.ema_pullback)
    e["atr"] = atr(e, CFG.atr_len)
    e["rsi"] = rsi(e["close"], CFG.rsi_len)

    rng = (e["high"] - e["low"]).replace(0, np.nan)
    e["bull_reversal"] = (e["close"] > e["open"]) & ((e["close"] - e["open"]) > 0.3 * rng)
    e["bear_reversal"] = (e["close"] < e["open"]) & ((e["open"] - e["close"]) > 0.3 * rng)
    e["lower_wick"] = (np.minimum(e["open"], e["close"]) - e["low"]) / rng
    e["upper_wick"] = (e["high"] - np.maximum(e["open"], e["close"])) / rng

    merged = align_trend_to_entry(e, t[["ts", "ema_fast", "ema_slow", "trend_up", "trend_dn"]])
    merged["ema_fast_prev"] = merged["ema_fast"].shift(1)
    merged = merged.dropna().reset_index(drop=True)
    return merged


def long_setup_ok(prev_row: pd.Series) -> bool:
    if not bool(prev_row.get("trend_up", False)):
        return False
    if not in_session(int(prev_row["ts"]), CFG.session_start_hh, CFG.session_end_hh):
        return False
    near_ema = (prev_row["low"] <= prev_row["ema_pb"] * 1.002) and (prev_row["close"] >= prev_row["ema_pb"] * 0.995)
    if not near_ema:
        return False
    if not (CFG.rsi_long_min <= float(prev_row["rsi"]) <= CFG.rsi_long_max):
        return False
    strong = (float(prev_row["lower_wick"]) >= 0.35) or bool(prev_row["bull_reversal"])
    return bool(strong)


def short_setup_ok(prev_row: pd.Series) -> bool:
    if not bool(prev_row.get("trend_dn", False)):
        return False
    if not in_session(int(prev_row["ts"]), CFG.session_start_hh, CFG.session_end_hh):
        return False
    rejected = (prev_row["high"] >= prev_row["ema_pb"] * 0.998) and (prev_row["close"] <= prev_row["ema_pb"] * 1.002)
    rsi_ok = (CFG.rsi_short_min <= float(prev_row["rsi"]) <= CFG.rsi_short_max)
    wick_or_bear = (float(prev_row["upper_wick"]) >= 0.35) or bool(prev_row["bear_reversal"])
    return bool(rejected and rsi_ok and wick_or_bear)


# ============================================================
# State (persistence) - engineering
# ============================================================
@dataclass
class BotState:
    peak_equity: float = 0.0
    last_15m_close: int = 0  # NOTE: stores BAR OPEN timestamp (open_ts), used for new-bar detection
    last_exit_time: Dict[str, int] = field(default_factory=dict)
    pending: Dict[str, PendingPlan] = field(default_factory=dict)
    trades: Dict[str, LiveTrade] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return {
            "peak_equity": float(self.peak_equity),
            "last_15m_close": int(self.last_15m_close),
            "last_exit_time": {k: int(v) for k, v in (self.last_exit_time or {}).items()},
            "pending": {k: asdict(v) for k, v in (self.pending or {}).items()},
            "trades": {k: asdict(v) for k, v in (self.trades or {}).items()},
        }

    @staticmethod
    def from_file(path: str) -> "BotState":
        if not os.path.exists(path):
            return BotState()
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        st = BotState(
            peak_equity=float(raw.get("peak_equity", 0.0)),
            last_15m_close=int(raw.get("last_15m_close", 0) or 0),
            last_exit_time=dict(raw.get("last_exit_time", {}) or {}),
            pending={},
            trades={},
        )
        for sym, pv in (raw.get("pending", {}) or {}).items():
            st.pending[sym] = PendingPlan(**pv)
        for sym, tv in (raw.get("trades", {}) or {}).items():
            st.trades[sym] = LiveTrade(**tv)
        return st

    def save(self, path: str):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_json(), f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


# ============================================================
# Risk helpers
# ============================================================
def per_trade_risk_usdt(entry_px: float, stop_px: float, qty: float) -> float:
    return float(abs(entry_px - stop_px) * abs(qty))


def total_open_risk_usdt(state: BotState) -> float:
    return float(sum(per_trade_risk_usdt(t.entry_px, t.stop, t.qty) for t in state.trades.values()))


def same_dir_open_risk_usdt(state: BotState, side: str) -> float:
    side = side.upper()
    return float(sum(per_trade_risk_usdt(t.entry_px, t.stop, t.qty) for t in state.trades.values() if t.side.upper() == side))


def get_leverage(sym: str) -> float:
    lev = CFG.leverage_map.get(sym, CFG.leverage)
    try:
        lev = float(lev)
    except Exception:
        lev = float(CFG.leverage)
    return max(1.0, lev)


def calc_position_size_by_risk(equity: float, entry_px: float, stop_px: float, risk_frac: float) -> float:
    risk_amount = float(equity) * float(risk_frac)
    per_unit = abs(entry_px - stop_px)
    if per_unit <= 0:
        return 0.0
    return max(0.0, risk_amount / per_unit)


# ============================================================
# Strategy: create pending plan (same as backtest)
# ============================================================
def make_pending_from_prev(prev: pd.Series) -> Optional[PendingPlan]:
    if pd.isna(prev.get("atr", np.nan)) or float(prev.get("atr", 0.0)) <= 0:
        return None

    ts = int(prev["ts"])
    atrv = float(prev["atr"])

    if long_setup_ok(prev):
        setup_high = float(prev["high"])
        setup_low = float(prev["low"])
        stop = setup_low - CFG.atr_stop_mult_long * atrv
        trigger = setup_high * 1.0005
        tp = trigger + CFG.rr_long * (trigger - stop)
        expire_ts = ts + CFG.pending_expire_bars * timeframe_to_ms(CFG.tf_entry)
        return PendingPlan(symbol="", side="LONG", setup_ts=ts, setup_idx=-1, trigger=trigger, stop=stop, tp=tp, expire_ts=expire_ts)

    if short_setup_ok(prev):
        setup_low = float(prev["low"])
        setup_high = float(prev["high"])
        stop = setup_high + CFG.atr_stop_mult_short * atrv
        trigger = setup_low * 0.9995
        tp = trigger - CFG.rr_short * (stop - trigger)
        expire_ts = ts + CFG.pending_expire_bars * timeframe_to_ms(CFG.tf_entry)
        return PendingPlan(symbol="", side="SHORT", setup_ts=ts, setup_idx=-1, trigger=trigger, stop=stop, tp=tp, expire_ts=expire_ts)

    return None


# ============================================================
# Main loop
# ============================================================
def main():
    ex = make_exchange()
    retry_call(ex.load_markets)

    # leverage setup
    for sym in CFG.symbols:
        set_leverage(ex, sym, get_leverage(sym))
        safe_sleep(0.05)

    data = DataCache(ex)
    state = BotState.from_file(CFG.state_path)

    # init peak equity
    equity0 = fetch_equity_usdt(ex)
    if state.peak_equity <= 0:
        state.peak_equity = float(equity0)
        state.save(CFG.state_path)

    tg_send(
        "✅ LIVE PORTFOLIO BOT STARTED\n"
        f"Exchange: Bybit USDT Linear | TF: {CFG.tf_entry}/{CFG.tf_trend}\n"
        f"MaxPos={CFG.max_positions} MaxSameDir={CFG.max_same_dir} Risk/trade={CFG.risk_per_trade:.3%}\n"
        f"RiskCap={CFG.total_risk_cap:.1%} DD_lock={CFG.dd_stop:.0%}\n"
        f"Session={('OFF' if CFG.session_start_hh is None else f'{CFG.session_start_hh}:00->{CFG.session_end_hh}:00 {CFG.tz}')}"
    )

    # warmup cache
    for sym in CFG.symbols:
        try:
            df15 = data.update(sym, CFG.tf_entry, limit=CFG.warmup_entry_bars)
            df1h = data.update(sym, CFG.tf_trend, limit=CFG.warmup_trend_bars)
            feats = build_features(df15, df1h)
            data.cache[(sym, "FEATS")] = feats
            safe_sleep(CFG.per_symbol_fetch_sleep)
        except Exception as e:
            tg_send(f"⚠️ Warmup failed {sym}: {type(e).__name__}: {e}")

    # helper: detect latest 15m close using first symbol
    def latest_bar_open_close_ts(clock_symbol: str) -> tuple[int, int]:
        df = data.update(clock_symbol, CFG.tf_entry, limit=200)
        if df.empty:
            return 0, 0
        open_ts = int(df["ts"].iloc[-1])  # CCXT: open time
        close_ts = open_ts + timeframe_to_ms(CFG.tf_entry)
        return open_ts, close_ts

    # periodic heartbeat timer
    last_hb = int(time.time())  # ✅ optional: start counting now

    while True:
        try:
            clock_sym = CFG.symbols[0]
            bar_open_ts, bar_close_ts = latest_bar_open_close_ts(clock_sym)

            # new-bar detection: compare OPEN times only
            if bar_open_ts <= int(state.last_15m_close or 0):
                # light TP watcher (only if TP is NOT on exchange, or TP掛單失敗需要本地備援)
                if (not CFG.tp_on_exchange) or any(tr.tp_fallback_local for tr in state.trades.values()):
                    for sym, tr in list(state.trades.items()):
                        try:
                            if CFG.tp_on_exchange and not tr.tp_fallback_local:
                                continue

                            px = fetch_mark_price(ex, sym)
                            if tr.side.upper() == "LONG" and px >= tr.tp:
                                place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                                cancel_bot_orders_symbol(ex, sym)
                                state.last_exit_time[sym] = int(time.time() * 1000)
                                tg_send(f"🎯 TP HIT EXIT {sym} LONG px={px:.6f} tp={tr.tp:.6f}")
                                state.trades.pop(sym, None)
                                state.pending.pop(sym, None)
                            elif tr.side.upper() == "SHORT" and px <= tr.tp:
                                place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                                cancel_bot_orders_symbol(ex, sym)
                                state.last_exit_time[sym] = int(time.time() * 1000)
                                tg_send(f"🎯 TP HIT EXIT {sym} SHORT px={px:.6f} tp={tr.tp:.6f}")
                                state.trades.pop(sym, None)
                                state.pending.pop(sym, None)
                        except Exception:
                            pass

                now = int(time.time())
                if now - last_hb >= int(CFG.heartbeat_sec):
                    equity = float(fetch_equity_usdt(ex))
                    state.peak_equity = max(state.peak_equity, equity)
                    dd = (state.peak_equity - equity) / state.peak_equity if state.peak_equity > 0 else 0.0
                    tg_send(
                        "🤖 HEARTBEAT\n"
                        f"Equity={equity:.2f} Peak={state.peak_equity:.2f} DD={dd*100:.2f}% {'(LOCKED)' if dd >= CFG.dd_stop else ''}\n"
                        f"OpenPos={len(state.trades)} Pending={len(state.pending)} OpenRisk≈{total_open_risk_usdt(state):.2f} USDT"
                    )
                    last_hb = now

                state.save(CFG.state_path)
                # sleep smarter: aim to wake up near next 15m close to reduce API calls
                sleep_s = float(CFG.poll_sec)
                try:
                    tf_ms = timeframe_to_ms(CFG.tf_entry)
                    now_ms = int(time.time() * 1000)
                    next_close = int(bar_close_ts) + tf_ms
                    to_next = max(1.0, (next_close - now_ms) / 1000.0 - 1.0)  # wake ~1s early
                    sleep_s = min(sleep_s, to_next)
                except Exception:
                    pass
                safe_sleep(sleep_s)
                continue

            # new bar detected -> delay a bit
            # wait until bar is actually closed, then delay
            if CFG.close_delay_sec > 0:
                now_ms = int(time.time() * 1000)
                wait_s = max(0.0, (bar_close_ts - now_ms) / 1000.0 + float(CFG.close_delay_sec))
                safe_sleep(wait_s)

            bar_ts = int(bar_close_ts)  # 用「收盤時間」當策略決策/紀錄時間
            state.last_15m_close = int(bar_open_ts)  # 狀態只存 open_ts（用來偵測新bar）

            # refresh all symbols data + features
            for sym in CFG.symbols:
                df15 = data.update(sym, CFG.tf_entry, limit=CFG.warmup_entry_bars)
                df1h = data.update(sym, CFG.tf_trend, limit=CFG.warmup_trend_bars)
                feats = build_features(df15, df1h)
                data.cache[(sym, "FEATS")] = feats
                safe_sleep(CFG.per_symbol_fetch_sleep)

            # equity + DD lock
            equity = float(fetch_equity_usdt(ex))
            state.peak_equity = max(state.peak_equity, equity)
            dd = (state.peak_equity - equity) / state.peak_equity if state.peak_equity > 0 else 0.0
            dd_lock = (dd >= CFG.dd_stop)

            # reconcile with exchange positions
            live_pos = fetch_live_positions(ex)

            # reconcile qty/protection
            for sym, tr in list(state.trades.items()):
                lp = live_pos.get(sym)
                if not lp:
                    continue
                ex_qty = float(lp.get("qty") or 0.0)
                ex_entry = float(lp.get("entry") or tr.entry_px or 0.0)
                if ex_qty <= 0:
                    continue

                qty_changed = abs(ex_qty - float(tr.qty)) > 1e-12
                if qty_changed:
                    old_qty = tr.qty
                    tr.qty = ex_qty
                    tr.entry_px = ex_entry
                    tg_send(f"🔄 RECONCILE QTY {sym} {tr.side} {old_qty:.6f}->{tr.qty:.6f} entry={tr.entry_px:.6f}")

                if qty_changed:
                    try:
                        cancel_bot_orders_symbol(ex, sym)
                    except Exception:
                        pass
                    tr.sl_tag = make_bot_tag(sym, "SL")
                    tr.tp_tag = make_bot_tag(sym, "TP") if CFG.tp_on_exchange else ""
                    try:
                        place_stop_market_reduceonly(ex, sym, tr.side, tr.qty, tr.stop, tag=tr.sl_tag)
                        if CFG.tp_on_exchange:
                            place_take_profit_market_reduceonly(ex, sym, tr.side, tr.qty, tr.tp, tag=tr.tp_tag)
                    except Exception as e:
                        tg_send(f"🛑 REPLACE PROTECTION FAIL {sym} err={e}")

                tr = verify_or_fallback_protection(ex, tr, CFG.tp_on_exchange, tg_prefix="RECON ")
                state.trades[sym] = tr

            # adopt missing
            for sym, lp in live_pos.items():
                if sym in state.trades:
                    continue

                side = (lp.get("side") or "").upper()
                entry = float(lp.get("entry") or 0.0)
                qty = float(lp.get("qty") or 0.0)
                if side not in ("LONG", "SHORT") or entry <= 0 or qty <= 0:
                    continue

                stop = None
                tp = None
                orders = fetch_open_orders_symbol(ex, sym)

                for o in orders:
                    info = o.get("info", {}) or {}
                    reduce_only = info.get("reduceOnly") or info.get("reduce_only") or o.get("reduceOnly")
                    trig = info.get("triggerPrice") or info.get("trigger_price") or o.get("triggerPrice")
                    if not reduce_only or trig is None:
                        continue
                    trig = float(trig)
                    if side == "LONG":
                        if trig < entry:
                            stop = trig
                        elif trig > entry:
                            tp = trig
                    else:
                        if trig > entry:
                            stop = trig
                        elif trig < entry:
                            tp = trig

                feats = data.get(sym, "FEATS")
                f = feats[feats["ts"] <= bar_ts] if feats is not None and not feats.empty else None
                row = f.iloc[-1] if f is not None and len(f) > 0 else None
                atrv = float(row.get("atr", np.nan)) if row is not None else np.nan

                if (stop is None or stop <= 0) and (not np.isnan(atrv) and atrv > 0):
                    stop = entry - CFG.atr_stop_mult_long * atrv if side == "LONG" else entry + CFG.atr_stop_mult_short * atrv

                init_dist = abs(entry - float(stop)) if stop is not None else 0.0

                if (tp is None or tp <= 0) and init_dist > 0:
                    tp = entry + CFG.rr_long * init_dist if side == "LONG" else entry - CFG.rr_short * init_dist

                try:
                    cancel_bot_orders_symbol(ex, sym)
                    sl_tag = make_bot_tag(sym, "SL")
                    tp_tag = make_bot_tag(sym, "TP") if CFG.tp_on_exchange else ""
                    if stop is not None and stop > 0:
                        place_stop_market_reduceonly(ex, sym, side, qty, float(stop), tag=sl_tag)
                    if CFG.tp_on_exchange and tp is not None and tp > 0:
                        place_take_profit_market_reduceonly(ex, sym, side, qty, float(tp), tag=tp_tag)
                except Exception:
                    sl_tag = ""
                    tp_tag = ""

                state.trades[sym] = LiveTrade(
                    symbol=sym, side=side, qty=qty, entry_px=entry, entry_ts=bar_ts, entry_idx=0,
                    stop=float(stop or 0.0), tp=float(tp or 0.0), lev=get_leverage(sym),
                    setup_ts=bar_ts, init_risk_dist=float(max(init_dist, 1e-9)), sl_tag=str(sl_tag), tp_tag=str(tp_tag),
                    tp_fallback_local=False
                )
                state.trades[sym] = verify_or_fallback_protection(ex, state.trades[sym], CFG.tp_on_exchange, tg_prefix="ADOPT ")
                tg_send(f"🧩 ADOPT {sym} {side} entry={entry:.6f} qty={qty}\nstop={float(stop or 0.0):.6f} tp={float(tp or 0.0):.6f}")

            # state has but exchange doesn't -> closed
            for sym in list(state.trades.keys()):
                if sym not in live_pos:
                    state.last_exit_time[sym] = bar_ts
                    tg_send(f"🏁 DETECT EXIT {sym}: position gone on exchange. Cooldown starts.")
                    state.trades.pop(sym, None)
                    state.pending.pop(sym, None)

            # manage exits (TIME / TREND_INVALID) on new 15m close
            for sym, tr in list(state.trades.items()):
                feats = data.get(sym, "FEATS")
                if feats is None or feats.empty:
                    continue
                f = feats[feats["ts"] <= bar_ts]
                if f.empty:
                    continue
                row = f.iloc[-1]

                # TIME
                bars_held = int((bar_ts - tr.entry_ts) // timeframe_to_ms(CFG.tf_entry))
                if bars_held >= CFG.max_hold_bars:
                    try:
                        place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                        cancel_bot_orders_symbol(ex, sym)
                        state.last_exit_time[sym] = bar_ts
                        tg_send(f"⏱️ TIME EXIT {sym} {tr.side} bars={bars_held}")
                        state.trades.pop(sym, None)
                        state.pending.pop(sym, None)
                        continue
                    except Exception as e:
                        tg_send(f"❌ TIME EXIT FAIL {sym}: {type(e).__name__}: {e}")

                # TREND_INVALID
                if CFG.exit_on_trend_flip:
                    if tr.side.upper() == "LONG" and (not bool(row.get("trend_up", True))):
                        try:
                            place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                            cancel_bot_orders_symbol(ex, sym)
                            state.last_exit_time[sym] = bar_ts
                            tg_send(f"🔁 TREND_INVALID EXIT {sym} LONG")
                            state.trades.pop(sym, None)
                            state.pending.pop(sym, None)
                            continue
                        except Exception as e:
                            tg_send(f"❌ TREND EXIT FAIL {sym}: {type(e).__name__}: {e}")
                    if tr.side.upper() == "SHORT" and (not bool(row.get("trend_dn", True))):
                        try:
                            place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                            cancel_bot_orders_symbol(ex, sym)
                            state.last_exit_time[sym] = bar_ts
                            tg_send(f"🔁 TREND_INVALID EXIT {sym} SHORT")
                            state.trades.pop(sym, None)
                            state.pending.pop(sym, None)
                            continue
                        except Exception as e:
                            tg_send(f"❌ TREND EXIT FAIL {sym}: {type(e).__name__}: {e}")

                # ✅ FIX: TP watcher at bar-close ONLY when needed
                if (not CFG.tp_on_exchange) or tr.tp_fallback_local:
                    try:
                        px = fetch_mark_price(ex, sym)
                        if tr.side.upper() == "LONG" and px >= tr.tp:
                            place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                            cancel_bot_orders_symbol(ex, sym)
                            state.last_exit_time[sym] = bar_ts
                            tg_send(f"🎯 TP EXIT {sym} LONG px={px:.6f} tp={tr.tp:.6f}")
                            state.trades.pop(sym, None)
                            state.pending.pop(sym, None)
                        elif tr.side.upper() == "SHORT" and px <= tr.tp:
                            place_reduceonly_market_exit(ex, sym, tr.side, tr.qty)
                            cancel_bot_orders_symbol(ex, sym)
                            state.last_exit_time[sym] = bar_ts
                            tg_send(f"🎯 TP EXIT {sym} SHORT px={px:.6f} tp={tr.tp:.6f}")
                            state.trades.pop(sym, None)
                            state.pending.pop(sym, None)
                    except Exception:
                        pass

            # pending expiry cleanup
            for sym, plan in list(state.pending.items()):
                if bar_ts > int(plan.expire_ts):
                    state.pending.pop(sym, None)

            # create new pending from prev bar
            for sym in CFG.symbols:
                if sym in state.trades:
                    continue
                feats = data.get(sym, "FEATS")
                if feats is None or len(feats) < 3:
                    continue
                f = feats[feats["ts"] <= bar_ts]
                if len(f) < 2:
                    continue
                prev = f.iloc[-2]
                plan = make_pending_from_prev(prev)
                if plan is None:
                    continue
                plan.symbol = sym
                plan.setup_idx = int(len(f) - 2)
                state.pending[sym] = plan

            # entries: trigger pending -> select -> risk/margin -> place orders
            if not dd_lock:
                open_risk = total_open_risk_usdt(state)

                candidates: List[Tuple[str, PendingPlan, float, np.ndarray, float]] = []
                for sym, plan in list(state.pending.items()):
                    if sym in state.trades:
                        state.pending.pop(sym, None)
                        continue
                    if len(state.trades) >= CFG.max_positions:
                        break

                    last_exit = state.last_exit_time.get(sym, 0)
                    if last_exit and (bar_ts - int(last_exit) < CFG.cooldown_minutes * 60_000):
                        continue

                    try:
                        px = fetch_mark_price(ex, sym)
                    except Exception:
                        continue

                    triggered = (px >= plan.trigger) if plan.side.upper() == "LONG" else (px <= plan.trigger)
                    if not triggered:
                        continue

                    if plan.armed_ts is None:
                        plan.armed_ts = bar_ts
                        state.pending[sym] = plan
                    delay_ms = int(CFG.brutal_entry_delay_bars) * timeframe_to_ms(CFG.tf_entry)
                    if delay_ms > 0 and (bar_ts - int(plan.armed_ts)) < delay_ms:
                        continue

                    if not hard_filter_rr_after_cost(float(plan.trigger), float(plan.stop), float(plan.tp)):
                        continue

                    feats = data.get(sym, "FEATS")
                    if feats is None or len(feats) < 3:
                        continue
                    f = feats[feats["ts"] <= bar_ts]
                    if len(f) < 2:
                        continue
                    prev = f.iloc[-2]
                    sc = score_plan(plan, prev)

                    df15 = data.cache.get((sym, CFG.tf_entry))
                    rs = build_return_series(df15, CFG.corr_lookback_bars) if isinstance(df15, pd.DataFrame) else np.array([])
                    candidates.append((sym, plan, sc, rs, px))

                candidates.sort(key=lambda x: x[2], reverse=True)

                selected: List[Tuple[str, PendingPlan, float]] = []
                selected_rs: List[np.ndarray] = []
                for sym, plan, sc, rs, px in candidates:
                    ok = True
                    for rs2 in selected_rs:
                        if abs(corr(rs, rs2)) >= CFG.corr_threshold:
                            ok = False
                            break
                    if not ok:
                        continue
                    selected.append((sym, plan, px))
                    selected_rs.append(rs)
                    if len(selected) >= CFG.max_entries_per_bar:
                        break

                for sym, plan, px in selected:
                    if sym in state.trades:
                        state.pending.pop(sym, None)
                        continue
                    if len(state.trades) >= CFG.max_positions:
                        break

                    if same_dir_open_risk_usdt(state, plan.side) >= equity * CFG.same_dir_risk_cap:
                        continue
                    if sum(1 for t in state.trades.values() if t.side.upper() == plan.side.upper()) >= CFG.max_same_dir:
                        continue

                    entry_est = float(plan.trigger)
                    stop_px = float(plan.stop)
                    qty = calc_position_size_by_risk(equity, entry_est, stop_px, CFG.risk_per_trade)
                    qty = round_amount(ex, sym, qty)
                    if qty <= 0:
                        state.pending.pop(sym, None)
                        continue

                    new_risk = abs(entry_est - stop_px) * abs(qty)
                    if (open_risk + new_risk) > equity * CFG.total_risk_cap:
                        continue

                    lev = get_leverage(sym)
                    notional = abs(qty) * entry_est
                    required_margin = notional / max(1.0, float(lev))
                    free_usdt = fetch_free_usdt(ex)
                    if required_margin > free_usdt * CFG.free_margin_buffer:
                        continue

                    try:
                        set_leverage(ex, sym, lev)
                        cancel_bot_orders_symbol(ex, sym)
                        place_market_entry(ex, sym, plan.side, qty)
                        safe_sleep(0.6)

                        live_pos2 = fetch_live_positions(ex)
                        if sym not in live_pos2:
                            tg_send(f"❌ ENTER FAIL {sym}: position not found after market order")
                            continue
                        actual_entry = float(live_pos2[sym]["entry"] or 0.0)
                        actual_qty = float(live_pos2[sym]["qty"] or qty)
                        if actual_entry <= 0 or actual_qty <= 0:
                            tg_send(f"❌ ENTER FAIL {sym}: invalid entry/qty from exchange")
                            continue

                        stop = float(plan.stop)
                        tp = float(plan.tp)
                        init_dist = abs(actual_entry - stop)
                        if init_dist <= 0:
                            init_dist = abs(entry_est - stop_px)

                        cancel_bot_orders_symbol(ex, sym)
                        sl_tag = make_bot_tag(sym, "SL")
                        tp_tag = make_bot_tag(sym, "TP") if CFG.tp_on_exchange else ""
                        place_stop_market_reduceonly(ex, sym, plan.side, actual_qty, stop, tag=sl_tag)
                        if CFG.tp_on_exchange:
                            place_take_profit_market_reduceonly(ex, sym, plan.side, actual_qty, tp, tag=tp_tag)

                        state.trades[sym] = LiveTrade(
                            symbol=sym, side=plan.side.upper(),
                            qty=float(actual_qty),
                            entry_px=float(actual_entry),
                            entry_ts=bar_ts,
                            entry_idx=int(plan.setup_idx + 1),
                            stop=float(stop),
                            tp=float(tp),
                            lev=float(lev),
                            setup_ts=int(plan.setup_ts),
                            init_risk_dist=float(max(init_dist, 1e-9)),
                            sl_tag=str(sl_tag),
                            tp_tag=str(tp_tag),
                            tp_fallback_local=False,
                        )
                        state.trades[sym] = verify_or_fallback_protection(ex, state.trades[sym], CFG.tp_on_exchange, tg_prefix="ENTER ")
                        open_risk += new_risk
                        state.pending.pop(sym, None)

                        tg_send(
                            f"🚀 ENTER {sym} {plan.side}\n"
                            f"entry={actual_entry:.6f} qty={actual_qty}\n"
                            f"stop={stop:.6f} tp={tp:.6f} (tp_on_ex={CFG.tp_on_exchange})\n"
                            f"risk≈{new_risk:.2f}USDT | DD={dd*100:.2f}%"
                        )
                    except Exception as e:
                        tg_send(f"❌ ENTER ERROR {sym}: {type(e).__name__}: {e}")

            tg_send(
                "🧾 BAR CLOSE\n"
                f"15m_close={pd.to_datetime(bar_ts, unit='ms', utc=True).tz_convert(CFG.tz)}\n"
                f"Equity={equity:.2f} Peak={state.peak_equity:.2f} DD={dd*100:.2f}% {'(LOCKED)' if dd_lock else ''}\n"
                f"OpenPos={len(state.trades)} Pending={len(state.pending)} OpenRisk≈{total_open_risk_usdt(state):.2f} USDT"
            )

            state.last_15m_close = int(bar_open_ts)
            state.save(CFG.state_path)


        except Exception as e:
            tg_send("❌ BOT ERROR:\n" + "".join(traceback.format_exception_only(type(e), e))[:3000])
            safe_sleep(5.0)


if __name__ == "__main__":
    main()
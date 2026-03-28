"""
Bybit Trading Skill - Configuration Management
"""
import os
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple
import yaml


@dataclass
class Config:
    # Exchange
    exchange: str = "bybit"
    market_type: str = "swap"
    category: str = "linear"

    # Symbols
    symbols: Tuple[str, ...] = (
        "BTC/USDT:USDT",
        "ETH/USDT:USDT",
        "SOL/USDT:USDT",
        "ADA/USDT:USDT",
        "DOGE/USDT:USDT",
        "LINK/USDT:USDT",
        "AVAX/USDT:USDT",
        "LTC/USDT:USDT",
        "ATOM/USDT:USDT"
    )

    # Timeframes
    tf_entry: str = "15m"
    tf_trend: str = "1h"

    # Risk Management
    leverage: float = 3.0
    leverage_map: Dict[str, float] = field(default_factory=dict)
    risk_per_trade: float = 0.008
    total_risk_cap: float = 0.04
    max_positions: int = 4
    max_same_dir: int = 2
    same_dir_risk_cap: float = 0.05

    # Stops
    atr_stop_mult_long: float = 1.6
    atr_stop_mult_short: float = 1.6
    rr_long: float = 1.6
    rr_short: float = 2.0

    # Entry Indicators
    ema_fast: int = 20
    ema_slow: int = 60
    ema_pullback: int = 20
    atr_len: int = 14
    rsi_len: int = 14
    rsi_long_min: float = 40.0
    rsi_long_max: float = 55.0
    rsi_short_min: float = 45.0
    rsi_short_max: float = 65.0

    # Fees
    fee_rate: float = 0.0006
    slippage_rate: float = 0.0002
    min_rr_after_cost: float = 1.20

    # Telegram
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None

    # Bot tag
    bot_tag_prefix: str = "LPB3"
    heartbeat_sec: int = 1800

    # Session filter (Taipei time)
    session_start_hh: Optional[int] = 20
    session_end_hh: Optional[int] = 2

    # DD circuit breaker
    dd_stop: float = 0.30
    cooldown_minutes: int = 30

    # Polling
    poll_sec: int = 90

    # State
    state_path: str = "state_live_portfolio.json"

    # Timezone
    tz: str = "Asia/Taipei"


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from YAML file."""
    config = Config()

    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
            if data:
                for key, value in data.items():
                    if hasattr(config, key):
                        setattr(config, key, value)

    # Override with environment variables
    if api_key := os.getenv("BYBIT_API_KEY"):
        pass  # API key will be passed directly

    if tg_token := os.getenv("TG_BOT_TOKEN"):
        config.telegram_bot_token = tg_token

    if tg_chat := os.getenv("TG_CHAT_ID"):
        config.telegram_chat_id = tg_chat

    return config


def save_config(config: Config, config_path: str = "config.yaml"):
    """Save configuration to YAML file."""
    data = {
        "exchange": config.exchange,
        "symbols": config.symbols,
        "leverage": config.leverage,
        "risk_per_trade": config.risk_per_trade,
        "total_risk_cap": config.total_risk_cap,
        "max_positions": config.max_positions,
        "session_start_hh": config.session_start_hh,
        "session_end_hh": config.session_end_hh,
        "dd_stop": config.dd_stop,
    }

    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)

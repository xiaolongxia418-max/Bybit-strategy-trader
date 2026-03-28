# Bybit Trading Skill

**Version:** 1.0.0  
**Author:** Your Name  
**Price:** $10/month  
**License:** Proprietary  

## Description

Automated cryptocurrency trading system for Bybit USDT Perpetual futures. Features EMA+RSI+ATR strategy with real-time execution, risk management, and Telegram notifications.

## Skill Structure

```
bybit-trading/
├── SKILL.md              # This file
├── README.md             # Detailed documentation
├── LICENSE               # Proprietary license
├── requirements.txt      # Python dependencies
├── cli.py                # CLI entry point
├── config.py             # Configuration management
├── logger.py             # Logging utilities
├── bybit_backtest.py     # Backtesting engine
├── bybit_live.py         # Live trading engine
└── examples/
    └── config.yaml       # Example configuration
```

## Commands

### backtest
Run strategy backtesting with historical data.

```bash
bybit-trading backtest --days 30
bybit-trading backtest --start 2024-01-01 --end 2024-03-01
```

### live
Start live trading with real money.

```bash
bybit-trading live --api-key KEY --api-secret SECRET
```

### status
Check current trading status.

```bash
bybit-trading status
```

## Environment Variables

Set these before running live trading:

```bash
export BYBIT_API_KEY="your_bybit_api_key"
export BYBIT_API_SECRET="your_bybit_api_secret"
export TG_BOT_TOKEN="your_telegram_bot_token"
export TG_CHAT_ID="your_telegram_chat_id"
```

## Configuration

Edit `config.yaml` or use CLI arguments:

| Parameter | Default | Description |
|-----------|---------|-------------|
| risk_per_trade | 0.008 | Risk per trade (0.8%) |
| max_positions | 4 | Maximum concurrent positions |
| leverage | 3.0 | Default leverage |
| dd_stop | 0.30 | Drawdown stop limit (30%) |
| session_start_hh | 20 | Session start hour (Taipei) |
| session_end_hh | 2 | Session end hour (Taipei) |

## Strategy

- **Trend Detection:** EMA 20/60 crossover on 1h timeframe
- **Entry Signals:** RSI + price action on 15m timeframe
- **Risk Management:** ATR-based stops, RR ratio 1.6-2.0
- **Position Control:** Max 4 positions, same direction max 2

## Dependencies

- Python 3.10+
- ccxt
- pandas
- numpy
- requests
- PyYAML

## Installation

```bash
pip install -r requirements.txt
```

## Warning

⚠️ **Trading involves substantial risk. Past performance does not guarantee future results. Use at your own risk.**

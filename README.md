# Bybit Auto Trader

Fully automated cryptocurrency futures trading system with built-in strategy. Backtesting verified and real-time trading ready.

## Features

### Core Features

- **Auto Entry** — EMA + RSI + ATR indicator strategy for entry signals
- **Smart Stop Loss** — ATR-based dynamic stops to minimize losses
- **Take Profit** — Auto-calculated RR ratio for optimal exits
- **Position Control** — Max 4 positions, same direction max 2
- **Risk Cap** — 0.8% per trade, 4% total risk
- **Correlation Filter** — Avoid high correlation in same-direction positions
- **Telegram Notifications** — Real-time trade status alerts

### Supported Exchanges

- **Bybit** USDT Perpetual (default)
- Extensible to other exchanges

### Supported Symbols

- BTC, ETH, SOL, ADA, DOGE, LINK, AVAX, LTC, ATOM

---

## Get Started with Bybit

👉 **[Sign up on Bybit with this link and get up to $6,135 in rewards](https://www.bybit.com/invite?ref=YD4AAG5)**

| Reward | Amount |
|--------|--------|
| Sign up | $10 USDC |
| Deposit $100 | $10 |
| First trade | $15 |
| Fee rebate | Up to 30% |

---

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Set API Key
export BYBIT_API_KEY="your_api_key"
export BYBIT_API_SECRET="your_api_secret"
```

---

## Usage

### Backtest

```bash
# Basic backtest (30 days)
bybit-trading backtest

# Custom days
bybit-trading backtest --days 60

# Custom date range
bybit-trading backtest --start 2024-01-01 --end 2024-03-01

# Custom risk
bybit-trading backtest --risk 0.01
```

### Live Trading

```bash
# Basic start
bybit-trading live --api-key YOUR_KEY --api-secret YOUR_SECRET

# Custom risk
bybit-trading live --api-key KEY --api-secret SEC --risk 0.01

# Dry run (simulate only)
bybit-trading live --api-key KEY --api-secret SEC --dry-run
```

### Check Status

```bash
bybit-trading status
```

---

## Configuration

Create `config.yaml` to customize parameters:

```yaml
# Exchange settings
exchange: bybit
symbols:
  - BTC/USDT:USDT
  - ETH/USDT:USDT

# Risk settings
risk_per_trade: 0.008
max_positions: 4
leverage: 3.0

# Session hours (Taipei time)
session_start_hh: 20
session_end_hh: 2

# Risk control
dd_stop: 0.30
```

---

## Strategy

### Entry Strategy

1. **Trend Detection** (1h EMA)
   - EMA 20 > EMA 60 → Uptrend
   - EMA 20 < EMA 60 → Downtrend

2. **Entry Conditions** (15m)
   - RSI at 40-55 (long) or 45-65 (short)
   - Price near EMA pullback
   - Candle pattern conditions met

3. **Position Sizing**
   - Calculate risk based on account equity
   - ATR for stop loss calculation

### Risk Management

| Parameter | Default | Description |
|-----------|---------|-------------|
| Per Trade Risk | 0.8% | Max loss per trade |
| Total Risk | 4% | All positions total risk cap |
| Max Positions | 4 | Max concurrent positions |
| Max Same Dir | 2 | Max same-direction positions |

---

## Sample Output

### Backtest Results

```
===== BACKTEST RESULTS =====
start_equity: 10000.0000
end_equity: 12450.5000
total_net_pct: 24.5050
max_dd_pct: 8.2340
trades: 47
winrate_pct: 61.7021
pf: 1.8532
avg_hold_bars: 8.5319
```

### Telegram Notification

```
🚀 ENTER BTC/USDT LONG
entry=50123.500000 qty=0.010
stop=49500.000000 tp=51500.000000
risk≈6.23USDT | DD=2.15%

🎯 TP HIT EXIT BTC/USDT LONG px=51523.45 tp=51500.000000
```

---

## Backtest Performance

| Metric | Result |
|--------|--------|
| Period | 20 days |
| Total Return | +18.18% |
| Win Rate | 63.75% |
| Profit Factor | 2.90 |
| Max Drawdown | 1.53% |

---

## Risk Disclaimer

⚠️ **Important**

- This tool is for reference only, not financial advice
- Trading involves risk, please trade responsibly
- Test in paper trading first
- You are responsible for your own trades
- Past performance does not guarantee future results

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| API connection failed | Check API Key is correct |
| Order failed | Check account balance |
| Stop loss not triggered | Verify stable network |
| Permission denied | Ensure API Key has futures trading permission |

---

## Tech Specs

- **Dependencies** — Python 3.10+, ccxt, pandas, numpy
- **Exchange** — Bybit USDT Perpetual
- **Timeframe** — 15m entry / 1h trend
- **Network** — Stable connection required

---

## License

This product is proprietary.
Redistribution or commercial use without authorization is prohibited.

---

## Support

- Issue reporting: Contact support team
- Update frequency: Monthly feature updates
- Technical support: Setup assistance available

---

## Bybit Bonus

👉 **[Use this link to sign up and get up to $6,135 in rewards](https://www.bybit.com/invite?ref=YD4AAG5)**

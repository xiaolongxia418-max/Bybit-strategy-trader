#!/usr/bin/env python3
"""
Bybit Trading Skill - CLI Interface
Usage: bybit-trading [command] [options]
"""
import argparse
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import load_config, Config
from logger import get_logger


def cmd_help():
    """Show help message."""
    help_text = """
Bybit Trading Skill - CLI Interface
==================================

Usage: bybit-trading [command] [options]

Commands:
    backtest    Run backtest with current settings
    live        Start live trading
    status      Check current status
    help        Show this help message

Backtest Options:
    --days N         Number of days to backtest (default: 30)
    --start DATE     Start date (YYYY-MM-DD)
    --end DATE       End date (YYYY-MM-DD)
    --risk R         Risk per trade (default: 0.008)
    --out FILE       Output file for trades (default: trades.csv)

Live Trading Options:
    --api-key KEY    Bybit API Key
    --api-secret SEC Bybit API Secret
    --risk R         Risk per trade (default: 0.008)
    --max-pos N      Maximum positions (default: 4)
    --dry-run        Run in dry-run mode (no actual trades)
    --session-off    Disable session time filter

Configuration:
    Config file: config.yaml (in current directory)
    Log file: trading.log

Examples:
    # Run backtest for 30 days
    bybit-trading backtest --days 30

    # Start live trading
    bybit-trading live --api-key YOUR_KEY --api-secret YOUR_SECRET

    # Dry-run with custom risk
    bybit-trading live --api-key KEY --api-secret SEC --risk 0.01 --dry-run

    # Backtest with custom date range
    bybit-trading backtest --start 2024-01-01 --end 2024-03-01
"""
    print(help_text)


def cmd_backtest(args, config: Config):
    """Run backtest."""
    try:
        from bybit_backtest import run_backtest, summarize

        # Override config with CLI args
        if args.days:
            config.backtest_days = args.days
        if args.start:
            config.backtest_start = args.start
        if args.end:
            config.backtest_end = args.end
        if args.risk:
            config.risk_per_trade = args.risk

        print(f"[BACKTEST] Starting backtest...")
        print(f"[BACKTEST] Days: {getattr(config, 'backtest_days', 30)}")

        # Run backtest
        trades, equity_curve = run_backtest(config)
        stats = summarize(trades, equity_curve, config)

        # Print results
        print("\n===== BACKTEST RESULTS =====")
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"{key}: {value:.4f}")
            else:
                print(f"{key}: {value}")

        # Save trades
        out_file = args.out or "trades.csv"
        import pandas as pd
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
            })
        pd.DataFrame(rows).to_csv(out_file, index=False)
        print(f"\n[Saved] Trades -> {out_file}")

    except ImportError as e:
        print(f"[ERROR] Missing dependency: {e}")
        print("[HINT] Run: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Backtest failed: {e}")
        sys.exit(1)


def cmd_live(args, config: Config):
    """Start live trading."""
    if not args.api_key or not args.api_secret:
        print("[ERROR] --api-key and --api-secret are required for live trading")
        print("[HINT] Run: bybit-trading live --api-key KEY --api-secret SECRET")
        sys.exit(1)

    # Override config with CLI args
    if args.risk:
        config.risk_per_trade = args.risk
    if args.max_pos:
        config.max_positions = args.max_pos

    # Set API credentials
    os.environ["BYBIT_API_KEY"] = args.api_key
    os.environ["BYBIT_API_SECRET"] = args.api_secret

    # Get logger
    logger = get_logger(
        bot_token=config.telegram_bot_token,
        chat_id=config.telegram_chat_id
    )

    print(f"[LIVE] Starting live trading...")
    print(f"[LIVE] Risk per trade: {config.risk_per_trade}")
    print(f"[LIVE] Max positions: {config.max_positions}")

    if args.dry_run:
        print("[LIVE] DRY RUN MODE - No actual trades will be made")

    try:
        from bybit_live import main as live_main

        # Send startup notification
        logger.send("🚀 LIVE TRADING BOT STARTED\n" + 
                   f"Risk: {config.risk_per_trade:.3%}\n" +
                   f"Max Positions: {config.max_positions}")

        # Run live bot
        live_main(config=config, logger=logger, dry_run=args.dry_run)

    except KeyboardInterrupt:
        print("\n[LIVE] Stopped by user")
        logger.send("⚠️ LIVE TRADING BOT STOPPED")
    except Exception as e:
        print(f"[ERROR] Live trading failed: {e}")
        logger.send(f"❌ LIVE TRADING ERROR: {e}")
        sys.exit(1)


def cmd_status(args, config: Config):
    """Check status."""
    state_file = config.state_path

    if os.path.exists(state_file):
        import json
        with open(state_file, "r") as f:
            state = json.load(f)
        print("===== TRADING STATUS =====")
        print(f"Peak Equity: {state.get('peak_equity', 0):.2f}")
        print(f"Open Positions: {len(state.get('trades', {}))}")
        print(f"Pending Plans: {len(state.get('pending', {}))}")
    else:
        print("[STATUS] No trading state found")
        print("[HINT] Run 'bybit-trading live' to start trading")


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("command", nargs="?", default="help")

    # Parse known args first
    args, unknown = parser.parse_known_args()

    # Handle subcommands
    if args.command == "help":
        cmd_help()
    elif args.command == "backtest":
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--days", type=int, default=30)
        parser.add_argument("--start", type=str, default=None)
        parser.add_argument("--end", type=str, default=None)
        parser.add_argument("--risk", type=float, default=None)
        parser.add_argument("--out", type=str, default=None)
        args = parser.parse_args()

        config = load_config()
        cmd_backtest(args, config)
    elif args.command == "live":
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--api-key", type=str, default=None)
        parser.add_argument("--api-secret", type=str, default=None)
        parser.add_argument("--risk", type=float, default=None)
        parser.add_argument("--max-pos", type=int, default=None)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--session-off", action="store_true")
        args = parser.parse_args()

        config = load_config()
        if args.session_off:
            config.session_start_hh = None
            config.session_end_hh = None
        cmd_live(args, config)
    elif args.command == "status":
        args = parser.parse_args()
        config = load_config()
        cmd_status(args, config)
    else:
        print(f"[ERROR] Unknown command: {args.command}")
        print("[HINT] Run 'bybit-trading help' for usage")
        sys.exit(1)


if __name__ == "__main__":
    main()

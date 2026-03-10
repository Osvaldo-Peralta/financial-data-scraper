"""
main.py
-------
CLI entry point for the Financial Data Scraper.
Punto de entrada CLI para el Financial Data Scraper.

Usage / Uso:
    python main.py --start 2023-01-01 --end 2024-01-01
    python main.py --tickers "^GSPC" "^NDX" --interval 1wk
"""

import argparse
import asyncio
import sys
from datetime import date, datetime

from src.scraper.fetcher import fetch_all, SUPPORTED_TICKERS
from src.db.repository import Repository
from src.utils.config import Settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{value}'. Expected YYYY-MM-DD.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="financial-data-scraper",
        description=(
            "Fetch historical OHLCV data for financial indices and persist to PostgreSQL.\n"
            "Descarga datos históricos OHLCV de índices financieros y los persiste en PostgreSQL."
        ),
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=list(SUPPORTED_TICKERS.values()),
        metavar="SYMBOL",
        help="Yahoo Finance ticker symbols (default: ^GSPC ^NDX).",
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        default=date(2020, 1, 1),
        metavar="YYYY-MM-DD",
        help="Start date (default: 2020-01-01).",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        default=date.today(),
        metavar="YYYY-MM-DD",
        help="End date (default: today).",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        choices=["1d", "1wk", "1mo"],
        help="Data interval (default: 1d).",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=500,
        metavar="N",
        help="Rows per INSERT batch (default: 500).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but skip database write. Useful for testing.",
    )
    return parser


async def run(args: argparse.Namespace, settings: Settings) -> None:
    logger.info("=== Financial Data Scraper started ===")
    logger.info("Tickers : %s", args.tickers)
    logger.info("Range   : %s → %s", args.start, args.end)
    logger.info("Interval: %s", args.interval)

    # 1. Fetch / Descargar
    results = await fetch_all(
        tickers=args.tickers,
        start=args.start,
        end=args.end,
        interval=args.interval,
    )

    if not results:
        logger.error("No data fetched. Exiting.")
        sys.exit(1)

    if args.dry_run:
        for ticker, df in results.items():
            logger.info("[DRY-RUN] %s — %d rows (not saved)", ticker, len(df))
        return

    # 2. Persist / Persistir
    repo = Repository(settings)
    repo.connect()
    try:
        for ticker, df in results.items():
            saved = repo.save_dataframe(df, block_size=args.block_size)
            logger.info("Saved %d rows for %s", saved, ticker)
    finally:
        repo.disconnect()

    logger.info("=== Scraper finished successfully ===")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings()
    asyncio.run(run(args, settings))


if __name__ == "__main__":
    main()

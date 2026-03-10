"""
fetcher.py
----------
Async data fetcher for financial indices using Yahoo Finance.
Fetches historical OHLCV data in configurable date ranges.

Módulo de descarga asíncrona de datos financieros desde Yahoo Finance.
Obtiene datos históricos OHLCV (Open, High, Low, Close, Volume) en rangos de fecha configurables.
"""

import asyncio
import logging
from datetime import date
from typing import Optional

import yfinance as yf
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


# Supported tickers / Tickers soportados
SUPPORTED_TICKERS: dict[str, str] = {
    "SP500":   "^GSPC",   # S&P 500
    "NDX100":  "^NDX",    # Nasdaq-100
}


class FetchError(Exception):
    """Raised when a ticker fetch fails. / Se lanza cuando la descarga de un ticker falla."""


async def fetch_ticker(
    ticker: str,
    start: date,
    end: date,
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Download historical OHLCV data for a single ticker asynchronously.

    Descarga datos históricos OHLCV de un ticker de forma asíncrona.

    Args:
        ticker:   Yahoo Finance ticker symbol (e.g. '^GSPC').
        start:    Start date (inclusive).
        end:      End date (inclusive).
        interval: Data interval ('1d', '1wk', '1mo').

    Returns:
        DataFrame with columns: [open, high, low, close, volume].

    Raises:
        FetchError: If the download fails or returns empty data.
    """
    logger.info("Fetching %s from %s to %s [interval=%s]", ticker, start, end, interval)

    try:
        # yfinance is synchronous; run in a thread pool to keep the event loop free.
        # yfinance es síncrono; se ejecuta en un thread pool para no bloquear el event loop.
        loop = asyncio.get_running_loop()
        df: pd.DataFrame = await loop.run_in_executor(
            None,
            lambda: yf.download(
                ticker,
                start=str(start),
                end=str(end),
                interval=interval,
                progress=False,
                auto_adjust=True,
            ),
        )
    except Exception as exc:
        raise FetchError(f"Network error while fetching {ticker}: {exc}") from exc

    if df.empty:
        raise FetchError(f"No data returned for {ticker} in range {start} → {end}.")

    # Normalise column names / Normalizar nombres de columnas
    df.columns = [c.lower() for c in df.columns]
    df.index.name = "date"
    df = df.reset_index()
    df["ticker"] = ticker

    logger.info("Fetched %d rows for %s", len(df), ticker)
    return df[["date", "ticker", "open", "high", "low", "close", "volume"]]


async def fetch_all(
    tickers: Optional[list[str]] = None,
    start: date = date(2020, 1, 1),
    end: date = date.today(),
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    """
    Concurrently fetch multiple tickers and return results keyed by symbol.

    Descarga múltiples tickers de forma concurrente y devuelve un diccionario
    cuyas claves son los símbolos de los tickers.

    Args:
        tickers:  List of Yahoo Finance symbols. Defaults to all SUPPORTED_TICKERS.
        start:    Start date.
        end:      End date.
        interval: Data interval.

    Returns:
        Dict mapping ticker symbol → DataFrame.
    """
    if tickers is None:
        tickers = list(SUPPORTED_TICKERS.values())

    tasks = {ticker: fetch_ticker(ticker, start, end, interval) for ticker in tickers}

    results: dict[str, pd.DataFrame] = {}
    raw = await asyncio.gather(*tasks.values(), return_exceptions=True)

    for ticker, result in zip(tasks.keys(), raw):
        if isinstance(result, Exception):
            logger.error("Failed to fetch %s: %s", ticker, result)
        else:
            results[ticker] = result

    return results

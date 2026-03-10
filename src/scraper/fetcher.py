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
from typing import Optional, Dict
from functools import partial

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
    pass


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
        loop = asyncio.get_running_loop()
            
        # Uso de functools.partial para mayor claridad en el executor
        download_func = partial(
            yf.download,
            tickers=ticker,
            start=start,
            end=end,
            interval=interval,
            progress=False,
            auto_adjust=True
        )
            
        # 1. Cambiamos el tipo esperado a Optional[pd.DataFrame]
        raw_df: Optional[pd.DataFrame] = await loop.run_in_executor(None, download_func)

        # 2. Type Guard: Verificamos si es None antes de continuar
        if raw_df is None:
            raise FetchError(f"Yahoo Finance returned None for {ticker}")
        
        # A partir de aquí, el linter sabe que 'df' es pd.DataFrame
        df: pd.DataFrame = raw_df

    except Exception as exc:
            raise FetchError(f"Network error for {ticker}: {exc}") from exc

    if df.empty:
        raise FetchError(f"No data returned for {ticker} in range {start} → {end}.")

    # Si las columnas son MultiIndex, nos quedamos solo con el primer nivel (Open, High, etc.)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Normalise column names / Normalizar nombres de columnas
    df.columns = [str(c).lower() for c in df.columns]
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

        # Crear tareas
    tasks = [fetch_ticker(t, start, end, interval) for t in tickers]
        
    # gather con return_exceptions=True para no detener todo el proceso si uno falla
    # results_list es inferido como Sequence[pd.DataFrame | BaseException]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)

    results: dict[str, pd.DataFrame] = {}
    
    for ticker, res in zip(tickers, results_list):
        # Al usar BaseException, cubrimos TODO lo que no sea el retorno exitoso
        if isinstance(res, BaseException):
            logger.error("Failed to fetch %s: %s", ticker, res)
            continue
        
        # Ahora Pylance garantiza que res es pd.DataFrame
        results[ticker] = res

    return results

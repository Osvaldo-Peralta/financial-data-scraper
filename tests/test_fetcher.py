"""
test_fetcher.py
---------------
Unit tests for the async fetcher module.
Pruebas unitarias para el módulo de descarga asíncrona.
"""

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from src.scraper.fetcher import fetch_ticker, fetch_all, FetchError


# ── Fixtures ───────────────────────────────────────────────────────────────

def _sample_df() -> pd.DataFrame:
    """Minimal OHLCV DataFrame that mimics yfinance output (including MultiIndex)."""
    # Creamos un DataFrame que simula el MultiIndex de las versiones nuevas de yfinance
    data = {
        ("Open", "^GSPC"): [4700.0, 4720.0],
        ("High", "^GSPC"): [4750.0, 4760.0],
        ("Low", "^GSPC"): [4690.0, 4710.0],
        ("Close", "^GSPC"): [4740.0, 4750.0],
        ("Volume", "^GSPC"): [3_000_000, 3_100_000],
    }
    df = pd.DataFrame(data, index=pd.to_datetime([date(2024, 1, 2), date(2024, 1, 3)]))
    df.index.name = "Date"
    return df


# ── fetch_ticker ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_ticker_returns_expected_columns():
    """fetch_ticker should return a DataFrame with the canonical columns."""
    with patch("src.scraper.fetcher.yf.download", return_value=_sample_df()):
        df = await fetch_ticker("^GSPC", date(2024, 1, 1), date(2024, 1, 31))

    # Verificamos que el aplanamiento de columnas y normalización funcionó
    expected = ["date", "ticker", "open", "high", "low", "close", "volume"]
    assert list(df.columns) == expected
    assert df["ticker"].unique() == ["^GSPC"]
    assert len(df) == 2


@pytest.mark.asyncio
async def test_fetch_ticker_raises_on_empty_response():
    """fetch_ticker should raise FetchError when yfinance returns an empty DataFrame."""
    with patch("src.scraper.fetcher.yf.download", return_value=pd.DataFrame()):
        with pytest.raises(FetchError, match="No data returned"):
            await fetch_ticker("^GSPC", date(2024, 1, 1), date(2024, 1, 31))


@pytest.mark.asyncio
async def test_fetch_ticker_raises_on_network_error():
    """fetch_ticker should wrap network exceptions in FetchError."""
    with patch("src.scraper.fetcher.yf.download", side_effect=ConnectionError("timeout")):
        with pytest.raises(FetchError, match="Network error"):
            await fetch_ticker("^GSPC", date(2024, 1, 1), date(2024, 1, 31))


# ── fetch_all ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_all_returns_successful_tickers_only():
    """
    fetch_all should return only tickers that succeeded.
    Failed tickers should be logged but not raise exceptions.
    """
    good_df = _sample_df()

    # CORRECCIÓN: Usamos tickers (con 's') para coincidir con el llamado de yf.download
    # y lo hacemos un keyword argument opcional para evitar el TypeError.
    def mock_download(*args, **kwargs):
        ticker = kwargs.get("tickers") or (args[0] if args else None)
        if ticker == "^GSPC":
            return good_df
        raise ConnectionError("forced failure")

    with patch("src.scraper.fetcher.yf.download", side_effect=mock_download):
        results = await fetch_all(
            tickers=["^GSPC", "^NDX"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

    # Ahora results no debería estar vacío
    assert "^GSPC" in results
    assert "^NDX" not in results
    assert isinstance(results["^GSPC"], pd.DataFrame)


@pytest.mark.asyncio
async def test_fetch_all_defaults_to_supported_tickers():
    """fetch_all with no tickers argument should use SUPPORTED_TICKERS."""
    with patch("src.scraper.fetcher.yf.download", return_value=_sample_df()):
        results = await fetch_all(start=date(2024, 1, 1), end=date(2024, 1, 31))

    # Ambas keys por defecto (SP500 y NDX100) mapeadas a sus valores en el resultado
    assert len(results) == 2

# ── Test para Valores Nulos (NaN) ────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_ticker_handles_nan_values():
    """
    fetch_ticker should handle DataFrames containing NaN values.
    Financial data often has gaps that must be preserved or handled.
    """
    # Creamos un sample con un valor NaN en la columna 'Close'
    nan_data = {
        ("Open", "^GSPC"): [4700.0, 4720.0],
        ("High", "^GSPC"): [4750.0, 4760.0],
        ("Low", "^GSPC"): [4690.0, 4710.0],
        ("Close", "^GSPC"): [4740.0, None],  # <--- Valor nulo
        ("Volume", "^GSPC"): [3_000_000, 3_100_000],
    }
    df_with_nan = pd.DataFrame(
        nan_data, 
        index=pd.to_datetime([date(2024, 1, 2), date(2024, 1, 3)])
    )
    
    with patch("src.scraper.fetcher.yf.download", return_value=df_with_nan):
        df = await fetch_ticker("^GSPC", date(2024, 1, 1), date(2024, 1, 31))

    # Verificamos que el DataFrame se procesó a pesar del NaN
    assert len(df) == 2
    assert pd.isna(df.loc[1, "close"])
    assert df.loc[0, "close"] == 4740.0
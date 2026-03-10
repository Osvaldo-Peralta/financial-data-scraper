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
    """Minimal OHLCV DataFrame that mimics yfinance output."""
    return pd.DataFrame(
        {
            "Date":   [date(2024, 1, 2), date(2024, 1, 3)],
            "Open":   [4700.0, 4720.0],
            "High":   [4750.0, 4760.0],
            "Low":    [4690.0, 4710.0],
            "Close":  [4740.0, 4750.0],
            "Volume": [3_000_000, 3_100_000],
        }
    ).rename(columns=str.lower).rename(columns={"date": "Date"}).set_index("Date")


# ── fetch_ticker ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_ticker_returns_expected_columns():
    """fetch_ticker should return a DataFrame with the canonical columns."""
    with patch("src.scraper.fetcher.yf.download", return_value=_sample_df()):
        df = await fetch_ticker("^GSPC", date(2024, 1, 1), date(2024, 1, 31))

    assert list(df.columns) == ["date", "ticker", "open", "high", "low", "close", "volume"]
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

    def mock_download(ticker, **kwargs):
        if ticker == "^GSPC":
            return good_df
        raise ConnectionError("forced failure")

    with patch("src.scraper.fetcher.yf.download", side_effect=mock_download):
        results = await fetch_all(
            tickers=["^GSPC", "^NDX"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

    assert "^GSPC" in results
    assert "^NDX" not in results


@pytest.mark.asyncio
async def test_fetch_all_defaults_to_supported_tickers():
    """fetch_all with no tickers argument should use SUPPORTED_TICKERS."""
    with patch("src.scraper.fetcher.yf.download", return_value=_sample_df()):
        results = await fetch_all(start=date(2024, 1, 1), end=date(2024, 1, 31))

    # Both default tickers should be present
    assert len(results) == 2

"""
test_repository.py
------------------
Unit tests for the PostgreSQL repository (no real DB needed — fully mocked).
Pruebas unitarias para el repositorio PostgreSQL (sin BD real — completamente mockeado).
"""

from datetime import date
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest

from src.db.repository import Repository, DEFAULT_BLOCK_SIZE
from src.utils.config import Settings


# ── Helpers ────────────────────────────────────────────────────────────────

def _make_repo() -> Repository:
    settings = Settings(database_url="postgresql://test:test@localhost/test")
    repo = Repository(settings)
    # Inject a mock connection / Inyectar una conexión mock
    repo._conn = MagicMock()
    repo._conn.closed = False
    return repo


def _sample_df(rows: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date":   [date(2024, 1, i + 1) for i in range(rows)],
            "ticker": ["^GSPC"] * rows,
            "open":   [4700.0 + i for i in range(rows)],
            "high":   [4750.0 + i for i in range(rows)],
            "low":    [4690.0 + i for i in range(rows)],
            "close":  [4740.0 + i for i in range(rows)],
            "volume": [3_000_000 + i * 1000 for i in range(rows)],
        }
    )


# ── save_dataframe ─────────────────────────────────────────────────────────

def test_save_dataframe_returns_row_count():
    """save_dataframe should return the number of rows upserted."""
    repo = _make_repo()
    df = _sample_df(10)

    with patch("src.db.repository.psycopg2.extras.execute_values"):
        count = repo.save_dataframe(df, block_size=DEFAULT_BLOCK_SIZE)

    assert count == 10


def test_save_dataframe_processes_in_blocks():
    """save_dataframe should split rows into blocks of the configured size."""
    repo = _make_repo()
    df = _sample_df(7)
    block_size = 3  # expect 3 blocks: [3, 3, 1]

    with patch("src.db.repository.psycopg2.extras.execute_values") as mock_exec:
        repo.save_dataframe(df, block_size=block_size)

    # 3 calls expected (ceil(7/3) = 3)
    assert mock_exec.call_count == 3


def test_save_dataframe_empty_returns_zero():
    """save_dataframe with an empty DataFrame should return 0 without DB calls."""
    repo = _make_repo()

    with patch("src.db.repository.psycopg2.extras.execute_values") as mock_exec:
        count = repo.save_dataframe(pd.DataFrame(), block_size=DEFAULT_BLOCK_SIZE)

    assert count == 0
    mock_exec.assert_not_called()


def test_save_dataframe_rolls_back_on_error():
    """save_dataframe should rollback the transaction on any DB error."""
    repo = _make_repo()
    df = _sample_df(3)

    with patch(
        "src.db.repository.psycopg2.extras.execute_values",
        side_effect=Exception("DB error"),
    ):
        with pytest.raises(Exception, match="DB error"):
            repo.save_dataframe(df)

    repo._conn.rollback.assert_called_once()


# ── get_latest_date ────────────────────────────────────────────────────────

def test_get_latest_date_returns_string():
    """get_latest_date should return the date as a string when data exists."""
    repo = _make_repo()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (date(2024, 6, 15),)
    repo._conn.cursor.return_value = mock_cursor

    result = repo.get_latest_date("^GSPC")
    assert result == "2024-06-15"


def test_get_latest_date_returns_none_when_no_data():
    """get_latest_date should return None when the table has no rows for the ticker."""
    repo = _make_repo()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (None,)
    repo._conn.cursor.return_value = mock_cursor

    result = repo.get_latest_date("^NDX")
    assert result is None

"""
repository.py
-------------
PostgreSQL repository for persisting financial OHLCV data.
Uses block-based inserts and UPSERT logic to guarantee idempotency and
transactional integrity.

Repositorio PostgreSQL para persistir datos financieros OHLCV.
Utiliza inserciones por bloques y lógica UPSERT para garantizar
idempotencia e integridad transaccional.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

from src.utils.config import Settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# DDL — created once on startup / se crea una vez al iniciar
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_data (
    id         BIGSERIAL PRIMARY KEY,
    date       DATE        NOT NULL,
    ticker     VARCHAR(20) NOT NULL,
    open       NUMERIC(18, 6),
    high       NUMERIC(18, 6),
    low        NUMERIC(18, 6),
    close      NUMERIC(18, 6),
    volume     BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT market_data_date_ticker_uq UNIQUE (date, ticker)
);
CREATE INDEX IF NOT EXISTS idx_market_data_ticker_date ON market_data (ticker, date);
"""

_UPSERT_SQL = """
INSERT INTO market_data (date, ticker, open, high, low, close, volume)
VALUES %s
ON CONFLICT (date, ticker) DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    volume     = EXCLUDED.volume,
    created_at = NOW();
"""

# Number of rows per INSERT batch / Número de filas por lote de INSERT
DEFAULT_BLOCK_SIZE = 500


class Repository:
    """
    Handles all database operations for market data.

    Gestiona todas las operaciones de base de datos para datos de mercado.
    """

    def __init__(self, settings: Settings) -> None:
        self._dsn = settings.database_url
        self._conn: PgConnection | None = None

    # ── connection management / gestión de conexión ────────────────────────

    def connect(self) -> None:
        """Open a connection and ensure the schema exists."""
        logger.info("Connecting to PostgreSQL…")
        self._conn = psycopg2.connect(self._dsn)
        self._ensure_schema()
        logger.info("Connected.")

    def disconnect(self) -> None:
        """Close the connection gracefully."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Database connection closed.")

    @contextmanager
    def _transaction(self) -> Generator[None, None, None]:
        """
        Context manager that commits on success and rolls back on any error.

        Gestor de contexto que hace commit si todo va bien y rollback si ocurre un error.
        """
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # ── schema / esquema ───────────────────────────────────────────────────

    def _ensure_schema(self) -> None:
        with self._transaction():
            with self._conn.cursor() as cur:
                cur.execute(_CREATE_TABLE_SQL)
        logger.debug("Schema verified / Esquema verificado.")

    # ── public API ─────────────────────────────────────────────────────────

    def save_dataframe(
        self,
        df: pd.DataFrame,
        block_size: int = DEFAULT_BLOCK_SIZE,
    ) -> int:
        """
        Persist a DataFrame into market_data using block-based UPSERT.

        Persiste un DataFrame en market_data usando UPSERT por bloques.
        This is the key optimization that reduced backup time from >5 min to ~30 s.
        Esta es la optimización clave que redujo el tiempo de respaldo de >5 min a ~30 s.

        Args:
            df:         DataFrame with columns [date, ticker, open, high, low, close, volume].
            block_size: Number of rows per INSERT batch.

        Returns:
            Total number of rows upserted.
        """
        if df.empty:
            logger.warning("Empty DataFrame — nothing to save.")
            return 0

        rows = [
            (
                row["date"],
                row["ticker"],
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]),
            )
            for _, row in df.iterrows()
        ]

        total = 0
        with self._transaction():
            with self._conn.cursor() as cur:
                # Process in blocks / Procesar en bloques
                for start in range(0, len(rows), block_size):
                    block = rows[start : start + block_size]
                    psycopg2.extras.execute_values(cur, _UPSERT_SQL, block)
                    total += len(block)
                    logger.debug(
                        "Upserted block %d–%d (%d rows)",
                        start + 1,
                        start + len(block),
                        len(block),
                    )

        logger.info("Total rows upserted: %d", total)
        return total

    def get_latest_date(self, ticker: str) -> str | None:
        """
        Return the most recent date stored for a given ticker, or None.

        Retorna la fecha más reciente almacenada para un ticker, o None.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(date) FROM market_data WHERE ticker = %s;",
                (ticker,),
            )
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None

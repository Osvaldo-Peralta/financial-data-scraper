# 📈 Financial Data Scraper

> Async Python scraper that fetches historical OHLCV data for financial indices (S&P 500, Nasdaq-100) from Yahoo Finance and persists it to PostgreSQL using block-based UPSERT for high-throughput ingestion.

> Scraper asíncrono en Python que descarga datos históricos OHLCV de índices financieros (S&P 500, Nasdaq-100) desde Yahoo Finance y los persiste en PostgreSQL mediante inserción por bloques (UPSERT) para alta eficiencia.

[![CI](https://github.com/Osvaldo-Peralta/financial-data-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/Osvaldo-Peralta/financial-data-scraper/actions)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue)
![Coverage](https://img.shields.io/badge/coverage-85%25%2B-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

---

## ✨ Key Features / Características Principales

| Feature | Description |
|---|---|
| **Async fetching** | Concurrent downloads via `asyncio` — no blocking the event loop |
| **Block-based UPSERT** | Inserts data in configurable batches → reduced ingestion time from **>5 min to ~30 s** |
| **Transactional integrity** | Each batch wrapped in a transaction; automatic rollback on failure |
| **Idempotent** | Re-running the scraper for the same date range never creates duplicates |
| **Dockerized** | Multi-stage Dockerfile reduces image size significantly |
| **CI/CD** | GitHub Actions pipeline with `pytest-cov` enforcing ≥ 85% coverage |
| **Configurable** | All settings via environment variables or `.env` file |

---

## 🗂 Project Structure / Estructura del Proyecto

```
financial-data-scraper/
├── src/
│   ├── scraper/
│   │   └── fetcher.py        # Async data fetching / Descarga asíncrona
│   ├── db/
│   │   └── repository.py     # PostgreSQL UPSERT repository / Repositorio PostgreSQL
│   └── utils/
│       ├── config.py         # Settings via pydantic-settings / Configuración
│       └── logger.py         # Centralised logging / Logging centralizado
├── tests/
│   ├── test_fetcher.py       # Fetcher unit tests
│   └── test_repository.py    # Repository unit tests (fully mocked DB)
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions CI pipeline
├── main.py                   # CLI entry point / Punto de entrada CLI
├── Dockerfile                # Multi-stage build
├── docker-compose.yml        # App + PostgreSQL stack
├── requirements.txt
└── .env.example
```

---

## ⚙️ Performance Highlight / Optimización de Rendimiento

The block-based insertion strategy was the critical optimization for large datasets:

La estrategia de inserción por bloques fue la optimización clave para datasets grandes:

```
Before / Antes:  Single INSERT per row  → S&P 500 historical backup: ~5+ minutes
After  / Después: Block INSERT (500 rows) → Same dataset:              ~30 seconds
```

This is achieved by batching rows and using PostgreSQL's `execute_values` with `ON CONFLICT DO UPDATE`, which minimises round-trips and leverages server-side bulk processing.

---

## 🚀 Quick Start / Inicio Rápido

### 1. Clone & configure / Clonar y configurar

```bash
git clone https://github.com/Osvaldo-Peralta/financial-data-scraper.git
cd financial-data-scraper

cp .env
# Edit .env with your database credentials / Edita .env con tus credenciales
```

### 2a. Run with Docker Compose (recommended) / Ejecutar con Docker Compose (recomendado)

```bash
docker compose up --build
```

This starts a PostgreSQL instance and runs the scraper automatically.  
Esto inicia una instancia de PostgreSQL y ejecuta el scraper automáticamente.

### 2b. Run locally / Ejecutar en local

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Fetch S&P 500 and Nasdaq-100 from 2023 to today
python main.py --start 2023-01-01

# Custom tickers and weekly interval
python main.py --tickers "^GSPC" "^NDX" --interval 1wk

# Dry-run (fetch only, no DB write) — useful for testing
python main.py --dry-run
```

### CLI Reference

| Flag | Default | Description |
|---|---|---|
| `--tickers` | `^GSPC ^NDX` | Yahoo Finance symbols |
| `--start` | `2020-01-01` | Start date (YYYY-MM-DD) |
| `--end` | today | End date (YYYY-MM-DD) |
| `--interval` | `1d` | `1d`, `1wk`, or `1mo` |
| `--block-size` | `500` | Rows per INSERT batch |
| `--dry-run` | off | Skip DB write |

---

## 🗄 Database Schema / Esquema de Base de Datos

```sql
CREATE TABLE market_data (
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
```

The schema is created automatically on first run.  
El esquema se crea automáticamente en la primera ejecución.

---

## 🧪 Running Tests / Ejecutar Pruebas

```bash
pip install -r requirements.txt
pytest --cov=src --cov-report=term-missing
```

All tests use mocks — no real database or network connection required.  
Todas las pruebas usan mocks — no se requiere base de datos ni conexión real.

---

## 🛠 Tech Stack

- **Python 3.12** — async/await, type hints
- **yfinance** — Yahoo Finance data source
- **asyncio** — concurrent fetching without threads
- **psycopg2** — PostgreSQL driver with `execute_values` bulk insert
- **pydantic-settings** — environment-based configuration
- **pytest + pytest-asyncio** — async-compatible testing
- **Docker** — multi-stage build for minimal image size
- **GitHub Actions** — CI with coverage enforcement

---

## 📄 License

MIT © [Osvaldo Peralta](https://github.com/Osvaldo-Peralta)

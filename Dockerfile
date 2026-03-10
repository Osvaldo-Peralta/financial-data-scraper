# ── Stage 1: builder ────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build tools only in the builder stage
# Instalar herramientas de compilación solo en la etapa de construcción
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Only runtime libraries — no build tools
# Solo librerías de ejecución — sin herramientas de compilación
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
# Copiar paquetes instalados desde el builder
COPY --from=builder /install /usr/local

# Copy application source
# Copiar código fuente
COPY src/ ./src/
COPY main.py .

# Non-root user for security / Usuario sin privilegios por seguridad
RUN useradd --no-create-home --shell /bin/false scraper
USER scraper

ENTRYPOINT ["python", "main.py"]

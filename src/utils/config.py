"""
config.py
---------
Centralised settings loaded from environment variables / .env file.
Configuración centralizada cargada desde variables de entorno o archivo .env.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings. All values can be overridden via environment variables.
    Configuración de la aplicación. Todos los valores pueden sobreescribirse con variables de entorno.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Database / Base de datos
    database_url: str = "postgresql://user:password@localhost:5432/market_db"

    # Scraper defaults / Valores predeterminados del scraper
    default_start_date: str = "2020-01-01"
    default_interval: str = "1d"
    block_size: int = 500

    # Logging
    log_level: str = "INFO"

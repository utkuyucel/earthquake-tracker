import logging
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ScrapingConfig:
    """Configuration for earthquake data scraping."""

    base_url: str = "http://www.koeri.boun.edu.tr/scripts/lst4.asp"
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    )


@dataclass(frozen=True)
class DataConfig:
    """Configuration for data processing and storage."""

    output_dir: str = "data"
    csv_filename: str = "earthquakes.csv"
    json_filename: str = "earthquakes.json"
    date_format: str = "%Y.%m.%d"
    time_format: str = "%H:%M:%S"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    encoding: str = "utf-8"


@dataclass(frozen=True)
class LoggingConfig:
    """Configuration for logging."""

    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = "scraper.log"


# Application configuration instances
SCRAPING = ScrapingConfig()
DATA = DataConfig()
LOGGING = LoggingConfig()


def setup_logging() -> logging.Logger:
    """Setup centralized logging configuration for the application."""
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, LOGGING.log_level),
        format=LOGGING.log_format,
        handlers=[],  # Clear any existing handlers
    )

    # Create formatter
    formatter = logging.Formatter(LOGGING.log_format)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # File handler if specified
    if LOGGING.log_file:
        file_handler = logging.FileHandler(LOGGING.log_file)
        file_handler.setFormatter(formatter)

    # Configure root logger with handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Remove any existing handlers
    root_logger.addHandler(console_handler)
    if LOGGING.log_file:
        root_logger.addHandler(file_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name."""
    return logging.getLogger(name)

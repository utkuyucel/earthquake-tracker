from .config import DATA, DATABASE, LOGGING, SCRAPING, get_logger, setup_logging
from .models import EarthquakeData
from .scraper import EarthquakeScraper, FileFormat

__version__ = "0.2.0"
__all__ = [
    "EarthquakeScraper",
    "FileFormat",
    "EarthquakeData",
    "SCRAPING",
    "DATA",
    "LOGGING",
    "DATABASE",
    "setup_logging",
    "get_logger",
]

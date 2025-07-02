"""
Earthquake Tracker - Simple earthquake data scraper for KOERI website.
"""

from .config import DATA, LOGGING, SCRAPING, get_logger, setup_logging
from .earthquake_scraper import EarthquakeData, EarthquakeScraper, FileFormat

__version__ = "0.1.0"
__all__ = [
    "EarthquakeScraper",
    "FileFormat",
    "EarthquakeData",
    "SCRAPING",
    "DATA",
    "LOGGING",
    "setup_logging",
    "get_logger",
]

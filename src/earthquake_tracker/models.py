"""
Data models for earthquake tracking.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class EarthquakeData:
    """Immutable earthquake data structure."""

    date: str
    time: str
    latitude: float
    longitude: float
    depth: float
    magnitude_md: Optional[float]
    magnitude_ml: Optional[float]
    magnitude_mw: Optional[float]
    location: str
    quality: str
    datetime_utc: datetime

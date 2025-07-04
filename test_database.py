#!/usr/bin/env python3
"""
Test script for earthquake tracker database functionality.
"""

import sys
from datetime import datetime

from src.earthquake_tracker import get_logger, setup_logging
from src.earthquake_tracker.database import DataWarehouse
from src.earthquake_tracker.models import EarthquakeData


def create_test_data():
    """Create sample earthquake data for testing."""
    return [
        EarthquakeData(
            date="2025.01.15",
            time="14:30:25",
            latitude=38.1234,
            longitude=27.5678,
            depth=12.5,
            magnitude_md=4.2,
            magnitude_ml=4.1,
            magnitude_mw=None,
            location="IZMIR KORFEZI",
            quality="C",
            datetime_utc=datetime(2025, 1, 15, 14, 30, 25),
        ),
        EarthquakeData(
            date="2025.01.15",
            time="15:45:12",
            latitude=39.9876,
            longitude=32.1234,
            depth=8.3,
            magnitude_md=3.8,
            magnitude_ml=3.7,
            magnitude_mw=3.9,
            location="ANKARA",
            quality="B",
            datetime_utc=datetime(2025, 1, 15, 15, 45, 12),
        ),
        # Test duplicate (same data)
        EarthquakeData(
            date="2025.01.15",
            time="14:30:25",
            latitude=38.1234,
            longitude=27.5678,
            depth=12.5,
            magnitude_md=4.2,
            magnitude_ml=4.1,
            magnitude_mw=None,
            location="IZMIR KORFEZI",
            quality="C",
            datetime_utc=datetime(2025, 1, 15, 14, 30, 25),
        ),
        # Test revised magnitude (same earthquake with different magnitude)
        EarthquakeData(
            date="2025.01.15",
            time="14:30:25",
            latitude=38.1234,
            longitude=27.5678,
            depth=12.5,
            magnitude_md=4.3,  # Revised magnitude
            magnitude_ml=4.2,  # Revised magnitude
            magnitude_mw=None,
            location="IZMIR KORFEZI",
            quality="B",  # Revised quality
            datetime_utc=datetime(2025, 1, 15, 14, 30, 25),
        ),
    ]


def test_bronze_layer(dw: DataWarehouse):
    """Test bronze layer functionality."""
    logger = get_logger(__name__)
    logger.info("Testing Bronze Layer...")

    test_earthquakes = create_test_data()

    # Test initial insert
    stats = dw.bronze_layer.insert_earthquakes(test_earthquakes)
    logger.info(f"Bronze layer stats: {stats}")

    # Test getting latest records
    latest_records = dw.bronze_layer.get_latest_records(limit=10)
    logger.info(f"Bronze layer has {len(latest_records)} records")

    for record in latest_records:
        magnitude = record.magnitude_ml or record.magnitude_md or "N/A"
        logger.info(f"  - {record.location} (M{magnitude}) - Hash: {record.row_hash[:8]}...")

    return stats


def test_silver_layer(dw: DataWarehouse):
    """Test silver layer functionality."""
    logger = get_logger(__name__)
    logger.info("Testing Silver Layer...")

    # Process bronze data into silver
    stats = dw.silver_layer.process_from_bronze()
    logger.info(f"Silver layer stats: {stats}")

    # Get latest earthquakes
    latest_earthquakes = dw.get_latest_earthquakes(limit=10)
    logger.info(f"Silver layer has {len(latest_earthquakes)} earthquakes")

    for earthquake in latest_earthquakes:
        magnitude = earthquake.magnitude_ml or earthquake.magnitude_md or "N/A"
        logger.info(
            f"  - {earthquake.location} (M{magnitude}) - Latest: {earthquake.latest_inserted_at}"
        )

    return stats


def test_full_pipeline(dw: DataWarehouse):
    """Test complete ingestion pipeline."""
    logger = get_logger(__name__)
    logger.info("Testing Full Pipeline...")

    test_earthquakes = create_test_data()
    stats = dw.ingest_earthquakes(test_earthquakes)

    logger.info(f"Pipeline stats: {stats}")
    return stats


def main():
    """Run database tests."""
    setup_logging()
    logger = get_logger(__name__)

    logger.info("Starting earthquake tracker database tests...")

    try:
        # Initialize data warehouse
        dw = DataWarehouse()
        logger.info("Data warehouse initialized successfully")

        # Test bronze layer
        bronze_stats = test_bronze_layer(dw)

        # Test silver layer
        silver_stats = test_silver_layer(dw)

        # Test full pipeline
        pipeline_stats = test_full_pipeline(dw)

        # Summary
        logger.info("Test Summary:")
        logger.info(f"Bronze: {bronze_stats}")
        logger.info(f"Silver: {silver_stats}")
        logger.info(f"Pipeline: {pipeline_stats}")

        logger.info("All tests completed successfully!")

        # Close connections
        dw.close()

        return 0

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Database management script for earthquake tracker.
"""

import sys
from pathlib import Path

from src.earthquake_tracker import DATABASE, get_logger, setup_logging
from src.earthquake_tracker.database import DataWarehouse


def start_database():
    """Start PostgreSQL database using Docker Compose."""
    import subprocess

    logger = get_logger(__name__)

    try:
        logger.info("Starting PostgreSQL database with Docker Compose...")

        # Check if docker-compose exists
        result = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True)

        if result.returncode != 0:
            logger.error("Docker Compose is not installed or not in PATH")
            return False

        # Start the database
        result = subprocess.run(
            ["docker-compose", "up", "-d", "postgres"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
        )

        if result.returncode == 0:
            logger.info("PostgreSQL database started successfully")
            logger.info(
                f"Database URL: postgresql://{DATABASE.username}:***@{DATABASE.host}:{DATABASE.port}/{DATABASE.database}"
            )
            return True
        else:
            logger.error(f"Failed to start database: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"Error starting database: {e}")
        return False


def stop_database():
    """Stop PostgreSQL database."""
    import subprocess

    logger = get_logger(__name__)

    try:
        logger.info("Stopping PostgreSQL database...")

        result = subprocess.run(
            ["docker-compose", "down"], capture_output=True, text=True, cwd=Path(__file__).parent
        )

        if result.returncode == 0:
            logger.info("PostgreSQL database stopped successfully")
            return True
        else:
            logger.error(f"Failed to stop database: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"Error stopping database: {e}")
        return False


def test_connection():
    """Test database connection and show layer statistics."""
    logger = get_logger(__name__)

    try:
        logger.info("Testing database connection...")

        # Initialize data warehouse (this will create schemas and tables)
        dw = DataWarehouse()

        # Get stats from bronze layer
        bronze_records = dw.bronze_layer.get_latest_records(limit=5)
        logger.info(f"Bronze layer: {len(bronze_records)} records (showing latest 5)")

        # Get stats from silver layer
        silver_records = dw.get_latest_earthquakes(limit=5)
        logger.info(f"Silver layer: {len(silver_records)} records (showing latest 5)")

        if bronze_records:
            logger.info("Latest bronze records:")
            for record in bronze_records[:3]:
                logger.info(
                    f"  - {record.location} (M{record.magnitude_ml or record.magnitude_md or 'N/A'}) at {record.datetime_utc}"
                )

        if silver_records:
            logger.info("Latest silver records:")
            for record in silver_records[:3]:
                logger.info(
                    f"  - {record.location} (M{record.magnitude_ml or record.magnitude_md or 'N/A'}) at {record.datetime_utc}"
                )

        dw.close()
        logger.info("Database connection test completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def main():
    """Main database management function."""
    setup_logging()
    logger = get_logger(__name__)

    if len(sys.argv) != 2:
        print("Usage:")
        print("  python db_manager.py start    - Start PostgreSQL database")
        print("  python db_manager.py stop     - Stop PostgreSQL database")
        print("  python db_manager.py test     - Test database connection")
        return 1

    command = sys.argv[1].lower()

    if command == "start":
        success = start_database()
        return 0 if success else 1
    elif command == "stop":
        success = stop_database()
        return 0 if success else 1
    elif command == "test":
        success = test_connection()
        return 0 if success else 1
    else:
        logger.error(f"Unknown command: {command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

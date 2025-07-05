import sys

from src.earthquake_tracker import DATABASE, get_logger, setup_logging
from src.earthquake_tracker.database import DataWarehouse


def test_connection():
    """Test RDS database connection and show layer statistics."""
    logger = get_logger(__name__)

    try:
        logger.info("Testing database connection...")
        logger.info(f"Connecting to: {DATABASE.host}:{DATABASE.port}/{DATABASE.database}")

        dw = DataWarehouse()

        bronze_records = dw.bronze_layer.get_latest_records(limit=5)
        logger.info(f"Bronze layer: {len(bronze_records)} records")

        silver_records = dw.get_latest_earthquakes(limit=5)
        logger.info(f"Silver layer: {len(silver_records)} records")

        if bronze_records:
            logger.info("Latest bronze records:")
            for record in bronze_records[:3]:
                magnitude = record.magnitude_ml or record.magnitude_md or 'N/A'
                logger.info(f"  - {record.location} (M{magnitude}) at {record.datetime_utc}")

        if silver_records:
            logger.info("Latest silver records:")
            for record in silver_records[:3]:
                magnitude = record.magnitude_ml or record.magnitude_md or 'N/A'
                logger.info(f"  - {record.location} (M{magnitude}) at {record.datetime_utc}")

        dw.close()
        logger.info("Database connection test completed successfully")
        return True

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


def main():
    """Main database management function."""
    setup_logging()
    logger = get_logger(__name__)

    if len(sys.argv) != 2:
        logger.error("Usage: python db_manager.py <test>")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)
    else:
        logger.error(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()

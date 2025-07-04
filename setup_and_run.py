import subprocess
import sys
import time

from src.earthquake_tracker import get_logger, setup_logging


def run_command(cmd, description="Running command"):
    """Run a shell command and return success status."""
    logger = get_logger(__name__)
    logger.info(f"{description}: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"✓ {description} completed successfully")
            return True
        else:
            logger.error(f"✗ {description} failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"✗ {description} failed: {e}")
        return False


def wait_for_database(max_attempts=30):
    """Wait for PostgreSQL database to be ready."""
    logger = get_logger(__name__)
    logger.info("Waiting for PostgreSQL database to be ready...")

    for attempt in range(max_attempts):
        try:
            # Try to connect using the test function
            from src.earthquake_tracker.database import DataWarehouse

            dw = DataWarehouse()
            dw.close()
            logger.info("✓ Database is ready!")
            return True
        except Exception:
            if attempt < max_attempts - 1:
                logger.info(
                    f"Database not ready yet, waiting... (attempt {attempt + 1}/{max_attempts})"
                )
                time.sleep(2)
            else:
                logger.error("✗ Database failed to start within timeout period")
                return False

    return False


def main():
    """Main setup and run function."""
    setup_logging()
    logger = get_logger(__name__)

    logger.info("🚀 Starting Earthquake Tracker v0.2")

    # Step 1: Start PostgreSQL database
    logger.info("📊 Starting PostgreSQL database...")
    if not run_command(["docker-compose", "up", "-d", "postgres"], "Starting PostgreSQL"):
        logger.error("Failed to start database. Make sure Docker is running.")
        return 1

    # Step 2: Wait for database to be ready
    if not wait_for_database():
        logger.error("Database is not responding")
        return 1

    # Step 3: Run the earthquake scraper
    logger.info("🌍 Running earthquake data scraper...")
    if not run_command([sys.executable, "run_scraper.py"], "Scraping earthquake data"):
        logger.error("Failed to scrape earthquake data")
        return 1

    # Step 4: Show database statistics
    logger.info("📈 Checking database statistics...")
    if not run_command([sys.executable, "db_manager.py", "test"], "Testing database"):
        logger.error("Failed to test database")
        return 1

    logger.info("✅ All operations completed successfully!")
    logger.info("\n" + "=" * 60)
    logger.info("🎉 Earthquake Tracker is now running!")
    logger.info("💾 Data is stored in PostgreSQL database")
    logger.info("🔍 Bronze layer: Raw data with deduplication")
    logger.info("✨ Silver layer: Processed data with revision tracking")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())

from pathlib import Path

from src.earthquake_tracker import DATA, EarthquakeScraper, FileFormat, get_logger, setup_logging


def main():
    """Run the earthquake scraper with database storage."""
    setup_logging()
    logger = get_logger(__name__)

    logger.info("Starting earthquake data scraper...")

    # Create data directory if it doesn't exist
    Path(DATA.output_dir).mkdir(exist_ok=True)

    scraper = EarthquakeScraper()

    try:
        # Scrape earthquake data
        earthquakes = scraper.scrape()
        if not earthquakes:
            logger.error("✗ Failed to scrape earthquake data")
            print("Failed to scrape data")
            return

        # Save to database (bronze -> silver pipeline)
        logger.info("Saving earthquake data to database...")
        db_success = scraper.save(earthquakes, FileFormat.DATABASE)

        if db_success:
            logger.info("✓ Earthquake data successfully saved to database")
            print("Data saved to PostgreSQL database")
        else:
            logger.error("✗ Failed to save earthquake data to database")
            print("Failed to save data to database")

        # Also save as CSV for backup/reference
        csv_success = scraper.save(earthquakes, FileFormat.CSV)
        if csv_success:
            csv_path = Path(DATA.output_dir) / DATA.csv_filename
            logger.info(f"✓ Backup CSV file saved to: {csv_path}")
            print(f"Backup CSV saved to: {csv_path}")

    finally:
        # Ensure database connections are properly closed
        scraper.close_database_connections()


if __name__ == "__main__":
    main()

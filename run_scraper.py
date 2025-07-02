from pathlib import Path

from src.earthquake_tracker import DATA, EarthquakeScraper, FileFormat, get_logger, setup_logging


def main():
    """Run the earthquake scraper."""
    setup_logging()
    logger = get_logger(__name__)

    logger.info("Starting earthquake data scraper...")

    # Create data directory if it doesn't exist
    Path(DATA.output_dir).mkdir(exist_ok=True)

    scraper = EarthquakeScraper()

    # Scrape earthquake data
    earthquakes = scraper.scrape()
    if not earthquakes:
        logger.error("✗ Failed to scrape earthquake data")
        print("Failed to scrape data")
        return

    # Save earthquake data as CSV (can be changed to FileFormat.JSON)
    success = scraper.save(earthquakes, FileFormat.CSV)
    if success:
        csv_path = Path(DATA.output_dir) / DATA.csv_filename
        logger.info(f"✓ Earthquake data successfully saved to: {csv_path}")
        print(f"Data saved to: {csv_path}")
    else:
        logger.error("✗ Failed to save earthquake data")
        print("Failed to save data")


if __name__ == "__main__":
    main()

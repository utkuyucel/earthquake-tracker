import logging
from pathlib import Path
from earthquake_scraper import EarthquakeScraper
from config import LOGGING, DATA

def setup_logging():
    """Setup basic logging configuration."""
    logging.basicConfig(
        level=getattr(logging, LOGGING.log_level),
        format=LOGGING.log_format
    )

def main():
    """Run the earthquake scraper."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting earthquake data scraper...")
    
    # Create data directory if it doesn't exist
    Path(DATA.output_dir).mkdir(exist_ok=True)
    
    scraper = EarthquakeScraper()
    success = scraper.scrape_and_save()
    
    if success:
        csv_path = Path(DATA.output_dir) / DATA.csv_filename
        logger.info(f"✓ Earthquake data successfully saved to: {csv_path}")
        print(f"Data saved to: {csv_path}")
    else:
        logger.error("✗ Failed to scrape earthquake data")
        print("Failed to scrape data")

if __name__ == "__main__":
    main()

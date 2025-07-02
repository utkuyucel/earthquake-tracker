import logging
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd

from config import SCRAPING, DATA, LOGGING


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


class EarthquakeDataParser:
    """Parse earthquake data from KOERI website text format."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def parse_magnitude(self, value: str) -> Optional[float]:
        """Parse magnitude value, handling '-.-' as None."""
        value = value.strip()
        if value == '-.-' or not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    
    def parse_coordinate(self, value: str) -> float:
        """Parse coordinate value."""
        try:
            return float(value.strip())
        except ValueError:
            self.logger.warning(f"Invalid coordinate value: {value}")
            return 0.0
    
    def parse_earthquake_line(self, line: str) -> Optional[EarthquakeData]:
        """Parse a single earthquake data line."""
        # Skip header lines and empty lines
        if not line.strip() or 'Tarih' in line or '-' * 5 in line:
            return None
            
        # Split by whitespace but preserve location names with spaces
        parts = line.strip().split()
        if len(parts) < 10:
            return None
            
        try:
            date_str = parts[0]
            time_str = parts[1]
            latitude = self.parse_coordinate(parts[2])
            longitude = self.parse_coordinate(parts[3])
            depth = self.parse_coordinate(parts[4])
            mag_md = self.parse_magnitude(parts[5])
            mag_ml = self.parse_magnitude(parts[6])
            mag_mw = self.parse_magnitude(parts[7])
            
            # Location is everything from parts[8] until the last part (quality)
            location_parts = parts[8:-1]
            location = ' '.join(location_parts)
            quality = parts[-1]
            
            # Create datetime object
            datetime_str = f"{date_str} {time_str}"
            datetime_utc = datetime.strptime(datetime_str, f"{DATA.date_format} {DATA.time_format}")
            
            return EarthquakeData(
                date=date_str,
                time=time_str,
                latitude=latitude,
                longitude=longitude,
                depth=depth,
                magnitude_md=mag_md,
                magnitude_ml=mag_ml,
                magnitude_mw=mag_mw,
                location=location,
                quality=quality,
                datetime_utc=datetime_utc
            )
            
        except (ValueError, IndexError) as e:
            self.logger.warning(f"Failed to parse line: {line.strip()}, error: {e}")
            return None


class EarthquakeScraper:
    """Scrape earthquake data from KOERI website."""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.session = self._create_session()
        self.parser = EarthquakeDataParser()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup structured logging."""
        logger = logging.getLogger(__name__)
        logger.setLevel(getattr(logging, LOGGING.log_level))
        
        formatter = logging.Formatter(LOGGING.log_format)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler if specified
        if LOGGING.log_file:
            file_handler = logging.FileHandler(LOGGING.log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        return logger
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with proper headers."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': SCRAPING.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        return session
    
    def fetch_earthquake_data(self) -> Optional[str]:
        """Fetch raw earthquake data from KOERI website."""
        for attempt in range(SCRAPING.max_retries):
            try:
                self.logger.info(f"Fetching data from {SCRAPING.base_url}, attempt {attempt + 1}")
                
                response = self.session.get(
                    SCRAPING.base_url,
                    timeout=SCRAPING.timeout
                )
                response.raise_for_status()
                
                # The response is in Windows-1254 encoding, decode properly
                response.encoding = 'windows-1254'
                
                self.logger.info(f"Successfully fetched data, size: {len(response.text)} characters")
                return response.text
                
            except requests.RequestException as e:
                self.logger.error(f"Request failed on attempt {attempt + 1}: {e}")
                if attempt < SCRAPING.max_retries - 1:
                    time.sleep(SCRAPING.retry_delay * (attempt + 1))
                    
        self.logger.error("Failed to fetch data after all retry attempts")
        return None
    
    def parse_earthquake_data(self, html_content: str) -> List[EarthquakeData]:
        """Parse earthquake data from HTML content."""
        self.logger.info("Parsing earthquake data from HTML content")
        
        # Find the data table in the HTML
        # The data appears to be in pre-formatted text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find the text content that contains earthquake data
        # Look for lines that match the earthquake data pattern
        text_content = soup.get_text()
        lines = text_content.split('\n')
        
        earthquakes = []
        data_section_started = False
        
        for line in lines:
            # Start parsing after finding the header
            if 'Tarih' in line and 'Saat' in line and 'Enlem' in line:
                data_section_started = True
                continue
            
            if not data_section_started:
                continue
                
            # Stop parsing when reaching the footer
            if 'telif hakları' in line.lower() or 'sitemizde yayımlanan' in line.lower():
                break
                
            earthquake = self.parser.parse_earthquake_line(line)
            if earthquake:
                earthquakes.append(earthquake)
        
        self.logger.info(f"Parsed {len(earthquakes)} earthquake records")
        return earthquakes
    
    def save_to_csv(self, earthquakes: List[EarthquakeData], filename: str) -> None:
        """Save earthquake data to CSV file."""
        if not earthquakes:
            self.logger.warning("No earthquake data to save")
            return
            
        # Convert to DataFrame
        data = []
        for eq in earthquakes:
            data.append({
                'date': eq.date,
                'time': eq.time,
                'datetime_utc': eq.datetime_utc.strftime(DATA.datetime_format),
                'latitude': eq.latitude,
                'longitude': eq.longitude,
                'depth_km': eq.depth,
                'magnitude_md': eq.magnitude_md,
                'magnitude_ml': eq.magnitude_ml,
                'magnitude_mw': eq.magnitude_mw,
                'location': eq.location,
                'quality': eq.quality
            })
        
        df = pd.DataFrame(data)
        
        # Ensure output directory exists
        output_path = Path(DATA.output_dir)
        output_path.mkdir(exist_ok=True)
        
        filepath = output_path / filename
        df.to_csv(filepath, index=False, encoding=DATA.encoding)
        
        self.logger.info(f"Saved {len(earthquakes)} records to {filepath}")
    
    def save_to_json(self, earthquakes: List[EarthquakeData], filename: str) -> None:
        """Save earthquake data to JSON file."""
        if not earthquakes:
            self.logger.warning("No earthquake data to save")
            return
            
        # Convert to list of dictionaries
        data = []
        for eq in earthquakes:
            data.append({
                'date': eq.date,
                'time': eq.time,
                'datetime_utc': eq.datetime_utc.isoformat(),
                'latitude': eq.latitude,
                'longitude': eq.longitude,
                'depth_km': eq.depth,
                'magnitude_md': eq.magnitude_md,
                'magnitude_ml': eq.magnitude_ml,
                'magnitude_mw': eq.magnitude_mw,
                'location': eq.location,
                'quality': eq.quality
            })
        
        # Ensure output directory exists
        output_path = Path(DATA.output_dir)
        output_path.mkdir(exist_ok=True)
        
        filepath = output_path / filename
        
        import json
        with open(filepath, 'w', encoding=DATA.encoding) as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved {len(earthquakes)} records to {filepath}")
    
    def scrape_and_save(self) -> bool:
        """Main method to scrape earthquake data and save to files."""
        self.logger.info("Starting earthquake data scraping")
        
        # Fetch raw data
        html_content = self.fetch_earthquake_data()
        if not html_content:
            return False
            
        # Parse earthquake data
        earthquakes = self.parse_earthquake_data(html_content)
        if not earthquakes:
            self.logger.error("No earthquake data found")
            return False
        
        # Save to files
        self.save_to_csv(earthquakes, DATA.csv_filename)
        self.save_to_json(earthquakes, DATA.json_filename)
        
        self.logger.info(f"Successfully scraped and saved {len(earthquakes)} earthquake records")
        return True


def main():
    scraper = EarthquakeScraper()
    success = scraper.scrape_and_save()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

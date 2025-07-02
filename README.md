# Earthquake Data Scraper

Simple Python script to scrape earthquake data from KOERI website and save as CSV or JSON.

## Usage

```bash
python run_scraper.py
```

## Files

- `config.py` - Configuration settings
- `earthquake_scraper.py` - Main scraper code  
- `run_scraper.py` - Simple runner script
- `data/earthquakes.csv` - Output data

## Requirements

```bash
pip install -r requirements.txt
```

## Example

```python
from earthquake_scraper import EarthquakeScraper, FileFormat

scraper = EarthquakeScraper()
earthquakes = scraper.scrape()
scraper.save(earthquakes, FileFormat.CSV)  # or FileFormat.JSON
```

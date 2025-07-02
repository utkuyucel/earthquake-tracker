# Earthquake Data Scraper

Simple Python script to scrape earthquake data from KOERI website and save as CSV or JSON.

## Usage

```bash
python run_scraper.py
```

## Installation

```bash
# Install dependencies
pip install -e .

# Install with development tools (with linting tools)
pip install -e ".[dev]"
```

## Files

- `config.py` - Configuration settings
- `earthquake_scraper.py` - Main scraper code  
- `run_scraper.py` - Simple runner script
- `pyproject.toml` - Project configuration and dependencies
- `data/earthquakes.csv` - Output data

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run linting and formatting
./lint.sh
```

## Example

```python
from src.earthquake_tracker import EarthquakeScraper, FileFormat

scraper = EarthquakeScraper()
earthquakes = scraper.scrape()
scraper.save(earthquakes, FileFormat.CSV)  # or FileFormat.JSON
```

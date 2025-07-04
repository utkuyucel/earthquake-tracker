# Earthquake Data Tracker

Python earthquake data scraper that fetches data from KOERI and stores it in a PostgreSQL data warehouse with bronze/silver layers.



## Quick Start

```bash
# Prerequisites: Docker, Docker Compose, Python 3.7+
git clone https://github.com/utkuyucel/earthquake-tracker
cd earthquake-tracker
pip install -e .
cp .env.example .env

# One-command setup and run
python setup_and_run.py
```

## Manual Operations

```bash
python db_manager.py start    # Start database
python run_scraper.py         # Run scraper  
python db_manager.py test     # Check stats
python db_manager.py stop     # Stop database
```

## Architecture

- **Bronze Layer**: Raw data with SHA-256 deduplication
- **Silver Layer**: Latest versions with revision tracking using `magnitude_ml`

## Key Files

- `setup_and_run.py` - Complete pipeline
- `run_scraper.py` - Scraper only
- `db_manager.py` - Database utilities
- `src/earthquake_tracker/` - Core package

## Configuration

Edit `src/earthquake_tracker/config.py` and `.env` file for database credentials.

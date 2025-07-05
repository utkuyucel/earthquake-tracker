# Earthquake Data Tracker

Python earthquake data scraper that fetches data from KOERI and stores it in AWS RDS PostgreSQL with bronze/silver layers.

## Quick Start

```bash
# Prerequisites: Python 3.7+, AWS RDS PostgreSQL instance
git clone https://github.com/utkuyucel/earthquake-tracker
cd earthquake-tracker
pip install -e .
cp .env.example .env

# Configure .env with your RDS credentials
# One-command setup and run
python setup_and_run.py
```

## Manual Operations

```bash
python db_manager.py test     # Test RDS connection and check stats
python run_scraper.py         # Run scraper only
```

## Architecture

- **Bronze Layer**: Raw data with SHA-256 deduplication
- **Silver Layer**: Latest versions with revision tracking using `magnitude_ml`
- **Database**: AWS RDS PostgreSQL with SSL connection

## Key Files

- `setup_and_run.py` - Complete pipeline
- `run_scraper.py` - Scraper only
- `db_manager.py` - Database utilities
- `src/earthquake_tracker/` - Core package

## Configuration

Edit `.env` file with your AWS RDS credentials:
```env
DB_HOST=your-rds-endpoint.region.rds.amazonaws.com
DB_PORT=5432
DB_NAME=earthquake_db
DB_USERNAME=postgres
DB_PASSWORD=your_password
```

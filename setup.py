#!/usr/bin/env python3
"""
Earthquake Tracker - Complete Setup Script

This script sets up everything needed for the earthquake tracker from scratch:
1. Checks Python environment and dependencies
2. Installs required packages if missing
3. Validates environment configuration
4. Sets up AWS RDS database infrastructure
5. Tests complete functionality
6. Provides usage instructions

Usage:
    python setup.py

This is the ONLY script you need to run to get everything working.
"""

import subprocess
import sys
from pathlib import Path


def run_command(command, description, check=True):
    """Run a shell command with error handling."""
    try:
        print(f"Running: {description}...")
        if isinstance(command, list):
            # Run as list to avoid shell interpretation
            result = subprocess.run(
                command, check=check, capture_output=True, text=True
            )
        else:
            # Run as string through shell
            result = subprocess.run(
                command, shell=True, check=check, capture_output=True, text=True
            )
        if result.stdout:
            print(f"   {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"FAILED: {e}")
        if e.stderr:
            print(f"   Error: {e.stderr.strip()}")
        return False


def check_virtual_environment():
    """Check if virtual environment is active."""
    print("Checking Python environment...")

    in_venv = hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )

    if in_venv:
        print("SUCCESS: Virtual environment detected")
        return True
    else:
        print("WARNING: Not in virtual environment")
        print("   Recommendation: Use a virtual environment for isolation")
        response = input("   Continue anyway? (y/N): ").lower()
        return response == "y"


def install_dependencies():
    """Install required Python packages."""
    print("Installing Python dependencies...")

    # Check if requirements.txt exists
    if Path("requirements.txt").exists():
        print("Found requirements.txt, installing dependencies...")
        cmd = ["pip", "install", "-r", "requirements.txt"]
        if not run_command(cmd, "Installing from requirements.txt"):
            print("FAILED: Could not install dependencies from requirements.txt")
            return False
    else:
        # Fallback to manual package list
        required_packages = [
            "psycopg2-binary>=2.9.0",
            "python-dotenv>=1.0.0",
            "requests>=2.31.0",
            "beautifulsoup4>=4.12.0",
            "pandas>=2.0.0",
            "python-dateutil>=2.8.0",
            "lxml>=4.9.0",
        ]

        for package in required_packages:
            package_name = package.split(">=")[0]
            try:
                __import__(package_name.replace("-", "_"))
                print(f"SUCCESS: {package_name} already installed")
            except ImportError:
                print(f"Installing {package}...")
                if not run_command(["pip", "install", package], f"Installing {package}"):
                    print(f"FAILED: Could not install {package}")
                    return False

    print("SUCCESS: All dependencies installed")
    return True


def check_project_files():
    """Check if all required project files exist."""
    print("Checking project files...")

    required_files = [
        "src/earthquake_tracker/__init__.py",
        "src/earthquake_tracker/config.py",
        "src/earthquake_tracker/database.py",
        "src/earthquake_tracker/scraper.py",
        "src/earthquake_tracker/models.py",
        "init.sql",
        "run_scraper.py",
        "db_manager.py",
    ]

    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)

    if missing_files:
        print("FAILED: Missing required files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        return False

    print("SUCCESS: All required project files found")
    return True


def setup_env_file():
    """Set up .env file with user input."""
    print("Setting up database configuration...")

    env_path = Path(".env")

    if env_path.exists():
        print("SUCCESS: .env file already exists")

        # Check if it has required variables
        with open(env_path) as f:
            content = f.read()

        required_vars = ["DB_HOST", "DB_USERNAME", "DB_PASSWORD"]
        missing_vars = [var for var in required_vars if f"{var}=" not in content]

        if not missing_vars:
            print("SUCCESS: .env file has all required variables")
            return True
        else:
            print(f"WARNING: Missing variables in .env: {missing_vars}")

    print("\nPlease provide your AWS RDS PostgreSQL connection details:")

    db_host = input("RDS Endpoint (e.g., database-1.xxx.rds.amazonaws.com): ").strip()
    if not db_host:
        print("ERROR: RDS endpoint is required")
        return False

    db_username = input("Username [postgres]: ").strip() or "postgres"

    db_password = input("Password: ").strip()
    if not db_password:
        print("ERROR: Password is required")
        return False

    env_content = f"""# AWS RDS Configuration
DB_HOST={db_host}
DB_PORT=5432
DB_NAME=postgres
DB_USERNAME={db_username}
DB_PASSWORD={db_password}
DB_SSLMODE=require
DB_CONNECT_TIMEOUT=30
"""

    with open(env_path, "w") as f:
        f.write(env_content)

    print("SUCCESS: .env file created")
    return True


def setup_database():
    """Set up the database infrastructure."""
    print("Setting up database infrastructure...")

    try:
        import psycopg2

        from src.earthquake_tracker.config import DATABASE

        # Test connectivity
        print("Testing RDS connectivity...")
        conn = psycopg2.connect(
            host=DATABASE.host,
            port=DATABASE.port,
            database="postgres",
            user=DATABASE.username,
            password=DATABASE.password,
            sslmode=DATABASE.sslmode,
            connect_timeout=10,
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"SUCCESS: Connected to PostgreSQL: {version[:50]}...")

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'earthquake_db'")
        db_exists = cursor.fetchone()

        if not db_exists:
            # Set autocommit for database creation
            conn.autocommit = True
            cursor.execute("CREATE DATABASE earthquake_db")
            print("SUCCESS: Database 'earthquake_db' created")
        else:
            print("SUCCESS: Database 'earthquake_db' already exists")

        cursor.close()
        conn.close()

        # Connect to earthquake_db and run init script
        print("Running initialization script...")
        conn = psycopg2.connect(
            host=DATABASE.host,
            port=DATABASE.port,
            database="earthquake_db",
            user=DATABASE.username,
            password=DATABASE.password,
            sslmode=DATABASE.sslmode,
            connect_timeout=DATABASE.connect_timeout,
        )

        cursor = conn.cursor()

        # Read and execute init.sql
        with open("init.sql") as sql_file:
            sql_script = sql_file.read()

        cursor.execute(sql_script)
        conn.commit()

        print("SUCCESS: Initialization script executed")

        # Verify schemas were created
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('bronze', 'silver')"
        )
        schemas = [row[0] for row in cursor.fetchall()]
        print(f"SUCCESS: Created schemas: {schemas}")

        cursor.close()
        conn.close()

        # Update .env file to use earthquake_db
        with open(".env") as f:
            lines = f.readlines()

        updated_lines = []
        for line in lines:
            if line.startswith("DB_NAME="):
                updated_lines.append("DB_NAME=earthquake_db\n")
            else:
                updated_lines.append(line)

        with open(".env", "w") as f:
            f.writelines(updated_lines)

        print("SUCCESS: Updated .env file to use earthquake_db")

        # Create tables and indexes
        print("Creating tables and indexes...")
        from src.earthquake_tracker.database import DataWarehouse

        dw = DataWarehouse()
        dw.close()

        print("SUCCESS: Database setup completed")
        return True

    except Exception as e:
        print(f"FAILED: Database setup failed: {e}")
        return False


def test_complete_system():
    """Test the complete earthquake tracker system."""
    print("Testing complete system...")

    # Test database connection
    if not run_command("python db_manager.py test", "Testing database connection"):
        return False

    # Test scraper import
    print("Testing earthquake scraper...")
    scraper_test = (
        'python -c "from src.earthquake_tracker.scraper import EarthquakeScraper; '
        "print('Scraper imported successfully')\""
    )
    if not run_command(scraper_test, "Testing scraper import"):
        return False

    print("SUCCESS: Complete system test passed")
    return True


def provide_usage_instructions():
    """Provide final usage instructions."""
    print("\n" + "=" * 60)
    print("EARTHQUAKE TRACKER SETUP COMPLETED")
    print("=" * 60)

    print("\nYour earthquake tracker is ready to use!")

    print("\nAvailable commands:")
    print("   python run_scraper.py           # Scrape latest earthquake data")
    print("   python db_manager.py test       # Test database connection")
    print("   python test_database.py         # Test database functionality")
    print("   python setup_and_run.py         # Run complete pipeline")

    print("\nDatabase structure:")
    print("   Database: earthquake_db")
    print("   Schemas: bronze (raw data), silver (processed data)")
    print("   Tables: earthquakes (in both schemas)")

    print("\nTips:")
    print("   - Run the scraper regularly to get latest earthquake data")
    print("   - Check logs in scraper.log for troubleshooting")
    print("   - Use db_manager.py test to verify database connectivity")

    print("\nConfiguration:")
    print("   - Database settings: .env file")
    print("   - Application settings: src/earthquake_tracker/config.py")


def main():
    """Main setup function."""
    print("=" * 60)
    print("EARTHQUAKE TRACKER - COMPLETE SETUP")
    print("=" * 60)
    print("\nThis script will set up everything you need to run the earthquake tracker!")
    print("It will take care of dependencies, database setup, and configuration.\n")

    # Step 1: Check Python environment
    if not check_virtual_environment():
        sys.exit(1)

    # Step 2: Check project files
    if not check_project_files():
        print("\nERROR: Missing project files. Please ensure you have the complete project.")
        sys.exit(1)

    # Step 3: Install dependencies
    if not install_dependencies():
        print("\nERROR: Failed to install dependencies")
        sys.exit(1)

    # Step 4: Set up environment configuration
    if not setup_env_file():
        print("\nERROR: Failed to set up .env file")
        sys.exit(1)

    # Step 5: Set up database
    if not setup_database():
        print("\nERROR: Database setup failed")
        print("Please check your RDS configuration and try again")
        sys.exit(1)

    # Step 6: Test complete system
    if not test_complete_system():
        print("\nERROR: System test failed")
        sys.exit(1)

    # Step 7: Provide usage instructions
    provide_usage_instructions()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        sys.exit(1)

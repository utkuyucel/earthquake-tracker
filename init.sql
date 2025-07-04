-- Initialize earthquake database with required schemas

-- Create bronze schema for raw data with deduplication
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create silver schema for processed data
CREATE SCHEMA IF NOT EXISTS silver;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;

-- Set default search path
ALTER DATABASE earthquake_db SET search_path TO bronze, silver, public;

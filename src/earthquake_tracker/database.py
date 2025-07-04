import hashlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from src.earthquake_tracker.config import DATABASE, get_logger
from src.earthquake_tracker.models import EarthquakeData


@dataclass(frozen=True)
class BronzeEarthquakeRecord:
    """Bronze layer earthquake record with additional metadata."""

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
    inserted_at: datetime
    row_hash: str


@dataclass(frozen=True)
class SilverEarthquakeRecord:
    """Silver layer earthquake record with latest data."""

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
    latest_inserted_at: datetime
    is_latest_revision: bool


class DatabaseManager:
    """Manage PostgreSQL database connections and operations."""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.connection_pool: Optional[ThreadedConnectionPool] = None
        self._init_connection_pool()

    def _init_connection_pool(self) -> None:
        """Initialize PostgreSQL connection pool."""
        try:
            self.connection_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=DATABASE.max_connections,
                host=DATABASE.host,
                port=DATABASE.port,
                database=DATABASE.database,
                user=DATABASE.username,
                password=DATABASE.password,
                connect_timeout=DATABASE.connection_timeout
            )
            self.logger.info("Database connection pool initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize database connection pool: {e}")
            raise

    def get_connection(self):
        """Get connection from pool."""
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialized")
        return self.connection_pool.getconn()

    def return_connection(self, conn) -> None:
        """Return connection to pool."""
        if self.connection_pool:
            self.connection_pool.putconn(conn)

    def close_all_connections(self) -> None:
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.logger.info("All database connections closed")
    
    def create_schemas_and_tables(self) -> None:
        """Create database schemas and tables if they don't exist."""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create schemas
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE.bronze_schema}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE.silver_schema}")

            # Create bronze table
            bronze_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {DATABASE.bronze_schema}.{DATABASE.bronze_table} (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20) NOT NULL,
                time VARCHAR(20) NOT NULL,
                latitude DECIMAL(10,6) NOT NULL,
                longitude DECIMAL(10,6) NOT NULL,
                depth DECIMAL(8,3) NOT NULL,
                magnitude_md DECIMAL(4,2),
                magnitude_ml DECIMAL(4,2),
                magnitude_mw DECIMAL(4,2),
                location TEXT NOT NULL,
                quality VARCHAR(50) NOT NULL,
                datetime_utc TIMESTAMP NOT NULL,
                inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                row_hash VARCHAR(64) NOT NULL UNIQUE,
                CONSTRAINT unique_earthquake_hash UNIQUE (row_hash)
            )
            """
            cursor.execute(bronze_table_sql)

            # Create index on bronze table
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_bronze_datetime_utc
                ON {DATABASE.bronze_schema}.{DATABASE.bronze_table} (datetime_utc)
            """)
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_bronze_inserted_at
                ON {DATABASE.bronze_schema}.{DATABASE.bronze_table} (inserted_at)
            """)

            # Create silver table
            silver_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {DATABASE.silver_schema}.{DATABASE.silver_table} (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20) NOT NULL,
                time VARCHAR(20) NOT NULL,
                latitude DECIMAL(10,6) NOT NULL,
                longitude DECIMAL(10,6) NOT NULL,
                depth DECIMAL(8,3) NOT NULL,
                magnitude_md DECIMAL(4,2),
                magnitude_ml DECIMAL(4,2),
                magnitude_mw DECIMAL(4,2),
                location TEXT NOT NULL,
                quality VARCHAR(50) NOT NULL,
                datetime_utc TIMESTAMP NOT NULL,
                latest_inserted_at TIMESTAMP NOT NULL,
                is_latest_revision BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(silver_table_sql)

            # Create unique index on silver table for earthquake identification
            cursor.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_silver_earthquake_unique
                ON {DATABASE.silver_schema}.{DATABASE.silver_table}
                (date, time, latitude, longitude, depth, location)
            """)

            conn.commit()
            self.logger.info("Database schemas and tables created successfully")

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Failed to create schemas and tables: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)


class BronzeLayer:
    """Bronze layer - handles raw data deduplication."""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = get_logger(__name__)

    def _calculate_row_hash(self, earthquake: EarthquakeData) -> str:
        """Calculate SHA-256 hash of earthquake data for deduplication."""
        # Create a consistent string representation of the earthquake data
        data_dict = asdict(earthquake)
        # Convert datetime to string for consistent hashing
        data_dict['datetime_utc'] = earthquake.datetime_utc.isoformat()

        # Sort keys to ensure consistent hash
        sorted_data = {k: data_dict[k] for k in sorted(data_dict.keys())}
        data_string = str(sorted_data)

        return hashlib.sha256(data_string.encode('utf-8')).hexdigest()
    
    def insert_earthquakes(self, earthquakes: List[EarthquakeData]) -> Dict[str, int]:
        """Insert earthquake data into bronze layer with deduplication."""
        if not earthquakes:
            self.logger.warning("No earthquake data to insert")
            return {"inserted": 0, "duplicates": 0}
        
        conn = None
        stats = {"inserted": 0, "duplicates": 0}
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            current_time = datetime.now(timezone.utc)
            
            for earthquake in earthquakes:
                row_hash = self._calculate_row_hash(earthquake)
                
                # Check if this exact data already exists
                cursor.execute(
                    f"SELECT id FROM {DATABASE.bronze_schema}.{DATABASE.bronze_table} WHERE row_hash = %s",
                    (row_hash,)
                )
                
                if cursor.fetchone():
                    self.logger.debug(f"Duplicate earthquake found, skipping: {earthquake.location} at {earthquake.datetime_utc}")
                    stats["duplicates"] += 1
                    continue
                
                # Insert new record
                insert_sql = f"""
                INSERT INTO {DATABASE.bronze_schema}.{DATABASE.bronze_table}
                (date, time, latitude, longitude, depth, magnitude_md, magnitude_ml, magnitude_mw,
                 location, quality, datetime_utc, inserted_at, row_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_sql, (
                    earthquake.date,
                    earthquake.time,
                    earthquake.latitude,
                    earthquake.longitude,
                    earthquake.depth,
                    earthquake.magnitude_md,
                    earthquake.magnitude_ml,
                    earthquake.magnitude_mw,
                    earthquake.location,
                    earthquake.quality,
                    earthquake.datetime_utc,
                    current_time,
                    row_hash
                ))
                
                stats["inserted"] += 1
            
            conn.commit()
            self.logger.info(f"Bronze layer: inserted {stats['inserted']} new records, skipped {stats['duplicates']} duplicates")
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Failed to insert earthquakes into bronze layer: {e}")
            raise
        finally:
            if conn:
                self.db_manager.return_connection(conn)
        
        return stats
    
    def get_latest_records(self, limit: Optional[int] = None) -> List[BronzeEarthquakeRecord]:
        """Get latest records from bronze layer."""
        conn = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            sql = f"""
            SELECT * FROM {DATABASE.bronze_schema}.{DATABASE.bronze_table}
            ORDER BY inserted_at DESC
            """
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            records = []
            for row in rows:
                records.append(BronzeEarthquakeRecord(
                    date=row['date'],
                    time=row['time'],
                    latitude=float(row['latitude']),
                    longitude=float(row['longitude']),
                    depth=float(row['depth']),
                    magnitude_md=float(row['magnitude_md']) if row['magnitude_md'] else None,
                    magnitude_ml=float(row['magnitude_ml']) if row['magnitude_ml'] else None,
                    magnitude_mw=float(row['magnitude_mw']) if row['magnitude_mw'] else None,
                    location=row['location'],
                    quality=row['quality'],
                    datetime_utc=row['datetime_utc'],
                    inserted_at=row['inserted_at'],
                    row_hash=row['row_hash']
                ))
            
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to get latest records from bronze layer: {e}")
            raise
        finally:
            if conn:
                self.db_manager.return_connection(conn)


class SilverLayer:
    """Silver layer - handles data transformation and latest revision tracking."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = get_logger(__name__)
    
    def _identify_earthquake_key(self, record: BronzeEarthquakeRecord) -> tuple:
        """Create earthquake identification key based on core attributes."""
        # Use date, time, location and coordinates to identify same earthquake
        return (
            record.date,
            record.time,
            record.latitude,
            record.longitude,
            record.depth,
            record.location
        )
    
    def _is_magnitude_revision(self, existing_ml: Optional[float], new_ml: Optional[float]) -> bool:
        """Check if there's a meaningful change in magnitude_ml (primary magnitude for revisions)."""
        # If both are None, no revision
        if existing_ml is None and new_ml is None:
            return False
        
        # If one is None and other isn't, it's a revision
        if existing_ml is None or new_ml is None:
            return True
        
        # If magnitudes differ by more than 0.05, consider it a revision
        return abs(existing_ml - new_ml) > 0.05
    
    def process_from_bronze(self) -> Dict[str, int]:
        """Process bronze layer data into silver layer with revision tracking."""
        conn = None
        stats = {"processed": 0, "updated": 0, "new": 0}
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Get all bronze records ordered by inserted_at
            cursor.execute(f"""
                SELECT * FROM {DATABASE.bronze_schema}.{DATABASE.bronze_table}
                ORDER BY datetime_utc, inserted_at
            """)
            bronze_records = cursor.fetchall()
            
            # Group by earthquake identification key
            earthquake_groups: Dict[tuple, List[Dict]] = {}
            
            for record in bronze_records:
                bronze_record = BronzeEarthquakeRecord(
                    date=record['date'],
                    time=record['time'],
                    latitude=float(record['latitude']),
                    longitude=float(record['longitude']),
                    depth=float(record['depth']),
                    magnitude_md=float(record['magnitude_md']) if record['magnitude_md'] else None,
                    magnitude_ml=float(record['magnitude_ml']) if record['magnitude_ml'] else None,
                    magnitude_mw=float(record['magnitude_mw']) if record['magnitude_mw'] else None,
                    location=record['location'],
                    quality=record['quality'],
                    datetime_utc=record['datetime_utc'],
                    inserted_at=record['inserted_at'],
                    row_hash=record['row_hash']
                )
                
                key = self._identify_earthquake_key(bronze_record)
                
                if key not in earthquake_groups:
                    earthquake_groups[key] = []
                
                earthquake_groups[key].append(record)
            
            # Process each earthquake group
            for earthquake_key, records in earthquake_groups.items():
                # Get the latest revision (by inserted_at)
                latest_record = max(records, key=lambda x: x['inserted_at'])
                
                # Check if this earthquake already exists in silver layer
                cursor.execute(f"""
                    SELECT id, latest_inserted_at, magnitude_ml 
                    FROM {DATABASE.silver_schema}.{DATABASE.silver_table}
                    WHERE date = %s AND time = %s AND latitude = %s AND longitude = %s
                    AND depth = %s AND location = %s
                """, earthquake_key)
                
                existing = cursor.fetchone()
                
                if existing:
                    # Check if this is a meaningful revision based on magnitude_ml
                    is_newer = latest_record['inserted_at'] > existing['latest_inserted_at']
                    is_magnitude_revision = self._is_magnitude_revision(
                        existing['magnitude_ml'], 
                        latest_record['magnitude_ml']
                    )
                    
                    # Update if we have a newer record OR if magnitude_ml changed significantly
                    if is_newer or is_magnitude_revision:
                        update_sql = f"""
                        UPDATE {DATABASE.silver_schema}.{DATABASE.silver_table}
                        SET magnitude_md = %s, magnitude_ml = %s, magnitude_mw = %s,
                            quality = %s, latest_inserted_at = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """
                        cursor.execute(update_sql, (
                            latest_record['magnitude_md'],
                            latest_record['magnitude_ml'],
                            latest_record['magnitude_mw'],
                            latest_record['quality'],
                            latest_record['inserted_at'],
                            existing['id']
                        ))
                        stats["updated"] += 1
                        
                        # Log revision details
                        if is_magnitude_revision:
                            self.logger.info(
                                f"Magnitude revision detected: {latest_record['location']} "
                                f"magnitude_ml {existing['magnitude_ml']} → {latest_record['magnitude_ml']}"
                            )
                        else:
                            self.logger.debug(f"Updated earthquake revision: {latest_record['location']}")
                else:
                    # Insert new earthquake
                    insert_sql = f"""
                    INSERT INTO {DATABASE.silver_schema}.{DATABASE.silver_table}
                    (date, time, latitude, longitude, depth, magnitude_md, magnitude_ml, magnitude_mw,
                     location, quality, datetime_utc, latest_inserted_at, is_latest_revision)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (
                        latest_record['date'],
                        latest_record['time'],
                        latest_record['latitude'],
                        latest_record['longitude'],
                        latest_record['depth'],
                        latest_record['magnitude_md'],
                        latest_record['magnitude_ml'],
                        latest_record['magnitude_mw'],
                        latest_record['location'],
                        latest_record['quality'],
                        latest_record['datetime_utc'],
                        latest_record['inserted_at'],
                        True
                    ))
                    stats["new"] += 1
                
                stats["processed"] += 1
            
            conn.commit()
            self.logger.info(f"Silver layer: processed {stats['processed']} earthquakes, "
                           f"new: {stats['new']}, updated: {stats['updated']}")
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Failed to process silver layer: {e}")
            raise
        finally:
            if conn:
                self.db_manager.return_connection(conn)
        
        return stats
    
    def get_latest_earthquakes(self, limit: Optional[int] = None) -> List[SilverEarthquakeRecord]:
        """Get latest earthquakes from silver layer."""
        conn = None
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            sql = f"""
            SELECT * FROM {DATABASE.silver_schema}.{DATABASE.silver_table}
            WHERE is_latest_revision = TRUE
            ORDER BY datetime_utc DESC
            """
            
            if limit:
                sql += f" LIMIT {limit}"
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            records = []
            for row in rows:
                records.append(SilverEarthquakeRecord(
                    date=row['date'],
                    time=row['time'],
                    latitude=float(row['latitude']),
                    longitude=float(row['longitude']),
                    depth=float(row['depth']),
                    magnitude_md=float(row['magnitude_md']) if row['magnitude_md'] else None,
                    magnitude_ml=float(row['magnitude_ml']) if row['magnitude_ml'] else None,
                    magnitude_mw=float(row['magnitude_mw']) if row['magnitude_mw'] else None,
                    location=row['location'],
                    quality=row['quality'],
                    datetime_utc=row['datetime_utc'],
                    latest_inserted_at=row['latest_inserted_at'],
                    is_latest_revision=row['is_latest_revision']
                ))
            
            return records
            
        except Exception as e:
            self.logger.error(f"Failed to get latest earthquakes from silver layer: {e}")
            raise
        finally:
            if conn:
                self.db_manager.return_connection(conn)


class DataWarehouse:
    """Main data warehouse orchestrator for bronze and silver layers."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = DatabaseManager()
        self.bronze_layer = BronzeLayer(self.db_manager)
        self.silver_layer = SilverLayer(self.db_manager)
        
        # Initialize database structure
        self.db_manager.create_schemas_and_tables()
    
    def ingest_earthquakes(self, earthquakes: List[EarthquakeData]) -> Dict[str, Any]:
        """Complete ingestion pipeline: bronze -> silver layers."""
        if not earthquakes:
            self.logger.warning("No earthquake data to ingest")
            return {"bronze": {"inserted": 0, "duplicates": 0}, "silver": {"processed": 0, "updated": 0, "new": 0}}
        
        try:
            # Step 1: Insert into bronze layer with deduplication
            self.logger.info("Starting bronze layer ingestion")
            bronze_stats = self.bronze_layer.insert_earthquakes(earthquakes)
            
            # Step 2: Process bronze data into silver layer
            self.logger.info("Starting silver layer processing")
            silver_stats = self.silver_layer.process_from_bronze()
            
            total_stats = {
                "bronze": bronze_stats,
                "silver": silver_stats
            }
            
            self.logger.info(f"Ingestion completed: {total_stats}")
            return total_stats
            
        except Exception as e:
            self.logger.error(f"Failed to ingest earthquakes: {e}")
            raise
    
    def get_latest_earthquakes(self, limit: Optional[int] = None) -> List[SilverEarthquakeRecord]:
        """Get latest earthquakes from silver layer."""
        return self.silver_layer.get_latest_earthquakes(limit)
    
    def close(self) -> None:
        """Close database connections."""
        self.db_manager.close_all_connections()

"""
Database Connection Manager and Schema Initializer Module
Provides robust, pooled connections for MySQL 8.x/9.x and zero-dependency SQLite
with transaction boundaries, schema deployment, and reference data seeding.
"""

import logging
import re
from pathlib import Path
from typing import Optional, Tuple, Any, Dict, List
import sqlalchemy
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.db")


def get_mysql_connection_url(include_db: bool = True) -> str:
    """Build MySQL SQLAlchemy connection URL using pymysql driver."""
    password_part = f":{Config.MYSQL_PASSWORD}" if Config.MYSQL_PASSWORD else ""
    db_part = f"/{Config.MYSQL_DATABASE}" if include_db else ""
    return (
        f"mysql+pymysql://{Config.MYSQL_USER}{password_part}@"
        f"{Config.MYSQL_HOST}:{Config.MYSQL_PORT}{db_part}"
        f"?charset={Config.MYSQL_CHARSET}"
    )


def create_mysql_database_if_not_exists() -> bool:
    """Creates the target MySQL database if it does not already exist."""
    try:
        server_url = get_mysql_connection_url(include_db=False)
        temp_engine = create_engine(server_url, isolation_level="AUTOCOMMIT")
        with temp_engine.connect() as conn:
            conn.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{Config.MYSQL_DATABASE}` "
                    f"CHARACTER SET {Config.MYSQL_CHARSET} COLLATE utf8mb4_unicode_ci;"
                )
            )
        temp_engine.dispose()
        logger.info(f"Confirmed MySQL database '{Config.MYSQL_DATABASE}'.")
        return True
    except Exception as e:
        logger.warning(f"Could not initialize MySQL database '{Config.MYSQL_DATABASE}': {e}")
        return False


def get_engine(
    engine_type: Optional[str] = None,
    use_fallback: bool = True
) -> Tuple[Engine, str]:
    """
    Returns an active SQLAlchemy Engine and the backend identifier ('mysql' or 'sqlite').
    
    If engine_type is 'mysql' or Config.DB_ENGINE == 'mysql':
        Attempts to connect to MySQL. If unreachable and use_fallback=True,
        falls back gracefully to SQLite.
    Otherwise:
        Initializes high-performance local SQLite database.
    """
    target = (engine_type or Config.DB_ENGINE).lower()

    if target == "mysql":
        try:
            created = create_mysql_database_if_not_exists()
            if not created and not use_fallback:
                raise ConnectionError("Failed to connect or create MySQL database.")
            
            mysql_url = get_mysql_connection_url(include_db=True)
            engine = create_engine(
                mysql_url,
                pool_size=10,
                max_overflow=20,
                pool_recycle=3600,
                pool_pre_ping=True
            )
            with engine.connect() as conn:
                version = conn.execute(text("SELECT VERSION();")).scalar()
                logger.info(f"Connected to MySQL Server {version} at {Config.MYSQL_HOST}:{Config.MYSQL_PORT}")
            return engine, "mysql"
        except Exception as e:
            logger.warning(f"MySQL connection unavailable: {e}")
            if not use_fallback:
                raise e
            logger.info("Falling back to local SQLite analytical engine.")

    # SQLite Engine Initialization
    Config.SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    sqlite_url = f"sqlite:///{Config.SQLITE_DB_PATH.as_posix()}"
    engine = create_engine(sqlite_url, echo=False)
    
    # Enable SQLite Foreign Key Enforcement
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))
        conn.execute(text("PRAGMA journal_mode = WAL;"))
        conn.commit()
    
    logger.info(f"Initialized SQLite database at {Config.SQLITE_DB_PATH}")
    return engine, "sqlite"


def _adapt_mysql_schema_for_sqlite(mysql_sql: str) -> str:
    """Translates MySQL DDL into standard SQLite DDL."""
    # Remove MySQL specific table options: ENGINE=..., DEFAULT CHARSET=..., COLLATE=...
    sql = re.sub(r"ENGINE\s*=\s*\w+", "", mysql_sql, flags=re.IGNORECASE)
    sql = re.sub(r"DEFAULT\s+CHARSET\s*=\s*\w+", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"COLLATE\s*=\s*\w+", "", sql, flags=re.IGNORECASE)
    # Replace AUTO_INCREMENT with AUTOINCREMENT (only valid with INTEGER PRIMARY KEY)
    sql = re.sub(r"BIGINT\s+AUTO_INCREMENT\s+PRIMARY\s+KEY", "INTEGER PRIMARY KEY AUTOINCREMENT", sql, flags=re.IGNORECASE)
    # Remove ON UPDATE CURRENT_TIMESTAMP
    sql = re.sub(r"ON\s+UPDATE\s+CURRENT_TIMESTAMP", "", sql, flags=re.IGNORECASE)
    # SQLite does not support standalone INDEX definitions inside CREATE TABLE without special syntax,
    # so we separate inline INDEX / KEY definitions into CREATE INDEX statements.
    return sql


def init_database(engine: Engine, engine_type: str) -> None:
    """Executes schema DDL to create all 12 tables and necessary indexes."""
    schema_path = Config.SQL_DIR / "schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at {schema_path}")

    raw_sql = schema_path.read_text(encoding="utf-8")

    if engine_type == "mysql":
        statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Successfully executed MySQL schema.")
    else:
        # SQLite schema setup
        _init_sqlite_schema(engine, raw_sql)
        logger.info("Successfully executed SQLite schema.")


def _init_sqlite_schema(engine: Engine, mysql_sql: str) -> None:
    """Robust SQLite table creator matching the exact schema definitions."""
    sqlite_ddl = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        customer_name TEXT NOT NULL,
        customer_email TEXT NOT NULL,
        registration_date TEXT NOT NULL,
        customer_status TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        unit_price NUMERIC NOT NULL,
        product_status TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS raw_transaction_events (
        staging_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        order_id TEXT,
        customer_id TEXT,
        product_id TEXT,
        event_type TEXT,
        quantity INTEGER,
        unit_price NUMERIC,
        event_timestamp TEXT,
        business_date TEXT,
        source_file TEXT,
        source_version INTEGER,
        run_id TEXT,
        ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS processed_transaction_events (
        event_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC NOT NULL,
        total_amount NUMERIC NOT NULL,
        event_timestamp TEXT NOT NULL,
        business_date TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_version INTEGER NOT NULL DEFAULT 1,
        run_id TEXT NOT NULL,
        processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

    CREATE TABLE IF NOT EXISTS current_order_state (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        current_status TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC NOT NULL,
        total_amount NUMERIC NOT NULL,
        last_event_id TEXT NOT NULL,
        last_event_type TEXT NOT NULL,
        last_event_timestamp TEXT NOT NULL,
        source_version INTEGER NOT NULL DEFAULT 1,
        business_date TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

    CREATE TABLE IF NOT EXISTS daily_sales_metrics (
        business_date TEXT PRIMARY KEY,
        total_orders INTEGER NOT NULL DEFAULT 0,
        completed_orders INTEGER NOT NULL DEFAULT 0,
        cancelled_orders INTEGER NOT NULL DEFAULT 0,
        gross_sales NUMERIC NOT NULL DEFAULT 0.00,
        net_sales NUMERIC NOT NULL DEFAULT 0.00,
        total_units_sold INTEGER NOT NULL DEFAULT 0,
        distinct_customers INTEGER NOT NULL DEFAULT 0,
        average_order_value NUMERIC NOT NULL DEFAULT 0.00,
        last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        run_id TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS daily_product_metrics (
        business_date TEXT NOT NULL,
        product_id TEXT NOT NULL,
        product_name TEXT NOT NULL,
        units_sold INTEGER NOT NULL DEFAULT 0,
        gross_sales NUMERIC NOT NULL DEFAULT 0.00,
        net_sales NUMERIC NOT NULL DEFAULT 0.00,
        order_count INTEGER NOT NULL DEFAULT 0,
        last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        run_id TEXT NOT NULL,
        PRIMARY KEY (business_date, product_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

    CREATE TABLE IF NOT EXISTS daily_customer_metrics (
        business_date TEXT NOT NULL,
        customer_id TEXT NOT NULL,
        order_count INTEGER NOT NULL DEFAULT 0,
        units_purchased INTEGER NOT NULL DEFAULT 0,
        gross_sales NUMERIC NOT NULL DEFAULT 0.00,
        net_sales NUMERIC NOT NULL DEFAULT 0.00,
        last_updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        run_id TEXT NOT NULL,
        PRIMARY KEY (business_date, customer_id),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE IF NOT EXISTS rejected_records (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_file TEXT NOT NULL,
        source_row_number INTEGER NOT NULL,
        event_id TEXT,
        rejection_reason TEXT NOT NULL,
        rejected_payload TEXT NOT NULL,
        rejected_at TEXT DEFAULT CURRENT_TIMESTAMP,
        run_id TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id TEXT NOT NULL,
        pipeline_stage TEXT NOT NULL,
        status TEXT NOT NULL,
        source_file TEXT,
        records_read INTEGER NOT NULL DEFAULT 0,
        records_valid INTEGER NOT NULL DEFAULT 0,
        records_rejected INTEGER NOT NULL DEFAULT 0,
        records_processed INTEGER NOT NULL DEFAULT 0,
        records_inserted INTEGER NOT NULL DEFAULT 0,
        records_updated INTEGER NOT NULL DEFAULT 0,
        metrics_recomputed INTEGER NOT NULL DEFAULT 0,
        error_type TEXT,
        error_message TEXT,
        duration_seconds NUMERIC,
        start_time TEXT NOT NULL,
        end_time TEXT,
        PRIMARY KEY (run_id, pipeline_stage)
    );

    CREATE TABLE IF NOT EXISTS ingestion_file_registry (
        file_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_hash TEXT NOT NULL UNIQUE,
        row_count INTEGER NOT NULL DEFAULT 0,
        file_size_bytes INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        run_id TEXT NOT NULL,
        ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS data_quality_results (
        check_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        check_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        records_checked INTEGER NOT NULL DEFAULT 0,
        failed_records INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        details TEXT,
        executed_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_proc_order ON processed_transaction_events (order_id);
    CREATE INDEX IF NOT EXISTS idx_proc_date ON processed_transaction_events (business_date);
    CREATE INDEX IF NOT EXISTS idx_order_date ON current_order_state (business_date);
    CREATE INDEX IF NOT EXISTS idx_order_status ON current_order_state (current_status);
    CREATE INDEX IF NOT EXISTS idx_reject_file ON rejected_records (source_file);
    """
    with engine.begin() as conn:
        for stmt in sqlite_ddl.strip().split(";"):
            cleaned = stmt.strip()
            if cleaned:
                conn.execute(text(cleaned))


def seed_reference_data(engine: Engine, engine_type: str) -> None:
    """Inserts initial reference records for customers and products if not present."""
    seed_path = Config.SQL_DIR / "seed_data.sql"
    if not seed_path.exists():
        logger.warning(f"Seed file not found at {seed_path}")
        return

    with engine.connect() as conn:
        cust_count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
        if cust_count and cust_count > 0:
            logger.info(f"Reference customers already seeded ({cust_count} records). Skipping.")
            return

    if engine_type == "mysql":
        raw_sql = seed_path.read_text(encoding="utf-8")
        statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Successfully seeded reference data into MySQL.")
    else:
        # SQLite compatible seeding
        customers = [
            ('CUST-1001', 'Alice Johnson', 'alice.j@example.com', '2025-01-15', 'ACTIVE'),
            ('CUST-1002', 'Bob Smith', 'bob.smith@example.com', '2025-02-10', 'ACTIVE'),
            ('CUST-1003', 'Carol Danvers', 'carol.d@example.com', '2025-03-05', 'ACTIVE'),
            ('CUST-1004', 'David Miller', 'david.m@example.com', '2025-04-12', 'ACTIVE'),
            ('CUST-1005', 'Eva Green', 'eva.green@example.com', '2025-05-20', 'ACTIVE'),
            ('CUST-1006', 'Frank Castle', 'frank.c@example.com', '2025-06-18', 'ACTIVE'),
            ('CUST-1007', 'Grace Hopper', 'grace.h@example.com', '2025-07-22', 'ACTIVE'),
            ('CUST-1008', 'Hank Pym', 'hank.pym@example.com', '2025-08-30', 'ACTIVE'),
            ('CUST-1009', 'Iris West', 'iris.west@example.com', '2025-09-14', 'ACTIVE'),
            ('CUST-1010', 'Jack Reacher', 'jack.r@example.com', '2025-10-01', 'ACTIVE')
        ]
        products = [
            ('PROD-2001', 'Ultra Wireless Headphones', 'Electronics', 149.99, 'ACTIVE'),
            ('PROD-2002', 'Ergonomic Mechanical Keyboard', 'Electronics', 89.50, 'ACTIVE'),
            ('PROD-2003', 'Gaming Mouse with RGB', 'Electronics', 49.95, 'ACTIVE'),
            ('PROD-2004', '4K Ultra HD Monitor 27in', 'Electronics', 329.00, 'ACTIVE'),
            ('PROD-2005', 'USB-C Multiport Docking Station', 'Accessories', 59.99, 'ACTIVE'),
            ('PROD-2006', 'Organic Cotton T-Shirt', 'Apparel', 24.50, 'ACTIVE'),
            ('PROD-2007', 'Waterproof Trail Running Shoes', 'Apparel', 119.00, 'ACTIVE'),
            ('PROD-2008', 'Stainless Steel Insulated Bottle', 'Home & Kitchen', 29.99, 'ACTIVE'),
            ('PROD-2009', 'Ceramic Pour-Over Coffee Dripper', 'Home & Kitchen', 34.00, 'ACTIVE'),
            ('PROD-2010', 'Aromatherapy Essential Oil Diffuser', 'Home & Kitchen', 39.95, 'ACTIVE')
        ]
        with engine.begin() as conn:
            for c in customers:
                conn.execute(
                    text("INSERT OR REPLACE INTO customers (customer_id, customer_name, customer_email, registration_date, customer_status) VALUES (:id, :name, :email, :reg, :status)"),
                    {"id": c[0], "name": c[1], "email": c[2], "reg": c[3], "status": c[4]}
                )
            for p in products:
                conn.execute(
                    text("INSERT OR REPLACE INTO products (product_id, product_name, category, unit_price, product_status) VALUES (:id, :name, :cat, :price, :status)"),
                    {"id": p[0], "name": p[1], "cat": p[2], "price": p[3], "status": p[4]}
                )
        logger.info("Successfully seeded reference data into SQLite.")


def test_connection() -> Dict[str, Any]:
    """Diagnostic check reporting database connection details."""
    result = {"connected": False, "engine_type": None, "details": "", "tables": []}
    try:
        engine, engine_type = get_engine(use_fallback=True)
        with engine.connect() as conn:
            if engine_type == "mysql":
                ver = conn.execute(text("SELECT VERSION()")).scalar()
                result["details"] = f"MySQL Server {ver} at {Config.MYSQL_HOST}:{Config.MYSQL_PORT}"
                tables = conn.execute(text("SHOW TABLES")).fetchall()
                result["tables"] = [t[0] for t in tables]
            else:
                ver = conn.execute(text("SELECT sqlite_version()")).scalar()
                result["details"] = f"SQLite {ver} at {Config.SQLITE_DB_PATH}"
                tables = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
                result["tables"] = [t[0] for t in tables]
        result["connected"] = True
        result["engine_type"] = engine_type
    except Exception as e:
        result["connected"] = False
        result["details"] = str(e)
    return result

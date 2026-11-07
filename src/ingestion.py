"""
Data Ingestion Module
Handles safe CSV ingestion, checksum calculation, header validation,
empty-file handling, and ingestion registry tracking with transactional integrity.
"""

import hashlib
import logging
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import pandas as pd
from sqlalchemy import text, Engine

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.ingestion")

EXPECTED_COLUMNS = [
    "event_id",
    "order_id",
    "customer_id",
    "product_id",
    "event_type",
    "quantity",
    "unit_price",
    "event_timestamp",
    "business_date"
]

OPTIONAL_COLUMNS = [
    "source_file",
    "source_version"
]


class IngestionError(Exception):
    """Custom exception raised when file ingestion fails."""
    pass


def calculate_file_hash(file_path: Path) -> str:
    """Computes SHA-256 hash of a file for exact duplicate file detection."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


class FileIngestionManager:
    """Manages file pre-checks, registry tracking, and raw staging loading."""

    def __init__(self, engine: Engine, engine_type: str):
        self.engine = engine
        self.engine_type = engine_type

    def check_file_registry(self, file_path: Path, file_hash: str) -> Optional[Dict[str, Any]]:
        """Checks if the file hash has already been registered and processed."""
        query = text(
            "SELECT file_id, file_path, file_hash, row_count, status, run_id "
            "FROM ingestion_file_registry WHERE file_hash = :hash"
        )
        with self.engine.connect() as conn:
            row = conn.execute(query, {"hash": file_hash}).mappings().first()
            if row:
                return dict(row)
        return None

    def register_file_start(self, file_path: Path, file_hash: str, run_id: str, row_count: int, file_size: int) -> int:
        """Registers the file ingestion attempt with status 'IN_PROGRESS', updating if exists."""
        if self.engine_type == "mysql":
            upsert_sql = text("""
                INSERT INTO ingestion_file_registry (
                    file_path, file_name, file_hash, row_count, file_size_bytes, status, run_id
                ) VALUES (
                    :path, :name, :hash, :rows, :size, 'IN_PROGRESS', :run_id
                )
                ON DUPLICATE KEY UPDATE
                    file_path = VALUES(file_path),
                    file_name = VALUES(file_name),
                    row_count = VALUES(row_count),
                    file_size_bytes = VALUES(file_size_bytes),
                    status = 'IN_PROGRESS',
                    run_id = VALUES(run_id),
                    ingested_at = CURRENT_TIMESTAMP
            """)
        else:
            upsert_sql = text("""
                INSERT INTO ingestion_file_registry (
                    file_path, file_name, file_hash, row_count, file_size_bytes, status, run_id
                ) VALUES (
                    :path, :name, :hash, :rows, :size, 'IN_PROGRESS', :run_id
                )
                ON CONFLICT(file_hash) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_name = excluded.file_name,
                    row_count = excluded.row_count,
                    file_size_bytes = excluded.file_size_bytes,
                    status = 'IN_PROGRESS',
                    run_id = excluded.run_id,
                    ingested_at = CURRENT_TIMESTAMP
            """)

        with self.engine.begin() as conn:
            result = conn.execute(upsert_sql, {
                "path": str(file_path.resolve()),
                "name": file_path.name,
                "hash": file_hash,
                "rows": row_count,
                "size": file_size,
                "run_id": run_id
            })
            file_id = result.lastrowid
        return file_id or 1

    def update_file_status(self, file_hash: str, status: str) -> None:
        """Updates the ingestion status ('SUCCESS', 'FAILED') upon pipeline completion."""
        update_sql = text("UPDATE ingestion_file_registry SET status = :status WHERE file_hash = :hash")
        with self.engine.begin() as conn:
            conn.execute(update_sql, {"status": status, "hash": file_hash})

    def ingest_file(
        self,
        file_path: Path,
        run_id: str,
        force: bool = False
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validates file existence, encoding, schema headers, registers the file,
        and returns the raw DataFrame with audit metadata.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found at: {file_path}")

        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.warning(f"File {file_path.name} is completely empty (0 bytes).")
            # Create an empty DataFrame with expected columns
            df_empty = pd.DataFrame(columns=EXPECTED_COLUMNS + OPTIONAL_COLUMNS)
            meta = {
                "file_path": str(file_path),
                "file_hash": "EMPTY_FILE",
                "row_count": 0,
                "file_size_bytes": 0,
                "is_empty": True,
                "already_processed": False
            }
            return df_empty, meta

        file_hash = calculate_file_hash(file_path)
        existing = self.check_file_registry(file_path, file_hash)
        
        if existing and existing["status"] == "SUCCESS" and not force:
            logger.warning(
                f"File '{file_path.name}' with hash {file_hash[:8]} was already ingested successfully in run {existing['run_id']}."
            )
            meta = {
                "file_path": str(file_path),
                "file_hash": file_hash,
                "row_count": existing["row_count"],
                "file_size_bytes": file_size,
                "is_empty": False,
                "already_processed": True,
                "prior_run_id": existing["run_id"]
            }
            return pd.DataFrame(), meta

        # Read CSV with encoding detection fallback (UTF-8, UTF-8-SIG, Latin1)
        df_raw = None
        for enc in ["utf-8", "utf-8-sig", "latin1"]:
            try:
                df_raw = pd.read_csv(file_path, encoding=enc, dtype=str)
                break
            except UnicodeDecodeError:
                continue

        if df_raw is None:
            raise IngestionError(f"Unable to decode file {file_path} with supported encodings (UTF-8, Latin1).")

        # Handle header-only empty file (0 rows)
        if len(df_raw) == 0:
            logger.warning(f"File {file_path.name} contains headers but 0 data rows.")
            meta = {
                "file_path": str(file_path),
                "file_hash": file_hash,
                "row_count": 0,
                "file_size_bytes": file_size,
                "is_empty": True,
                "already_processed": False
            }
            return df_raw, meta

        # Header Validation
        present_cols = set(df_raw.columns)
        missing_cols = [c for c in EXPECTED_COLUMNS if c not in present_cols]
        if missing_cols:
            raise IngestionError(f"Missing required columns in {file_path.name}: {missing_cols}")

        unexpected_cols = [c for c in df_raw.columns if c not in EXPECTED_COLUMNS and c not in OPTIONAL_COLUMNS]
        if unexpected_cols:
            logger.warning(f"Detected unexpected additional columns: {unexpected_cols}. They will be preserved in raw staging.")

        # Ensure source_file and source_version defaults
        if "source_file" not in df_raw.columns:
            df_raw["source_file"] = file_path.name
        else:
            df_raw["source_file"] = df_raw["source_file"].fillna(file_path.name)

        if "source_version" not in df_raw.columns:
            df_raw["source_version"] = "1"
        else:
            df_raw["source_version"] = df_raw["source_version"].fillna("1")

        # Register file start
        self.register_file_start(file_path, file_hash, run_id, len(df_raw), file_size)

        meta = {
            "file_path": str(file_path),
            "file_hash": file_hash,
            "row_count": len(df_raw),
            "file_size_bytes": file_size,
            "is_empty": False,
            "already_processed": False
        }

        # Stage raw data into raw_transaction_events
        self.stage_raw_records(df_raw, run_id)

        return df_raw, meta

    def stage_raw_records(self, df: pd.DataFrame, run_id: str) -> None:
        """Appends all received rows into raw_transaction_events staging table."""
        if df.empty:
            return

        staging_df = pd.DataFrame()
        staging_df["event_id"] = df["event_id"]
        staging_df["order_id"] = df["order_id"]
        staging_df["customer_id"] = df["customer_id"]
        staging_df["product_id"] = df["product_id"]
        staging_df["event_type"] = df["event_type"]
        staging_df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        staging_df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
        staging_df["event_timestamp"] = df["event_timestamp"].astype(str)
        staging_df["business_date"] = df["business_date"].astype(str)
        staging_df["source_file"] = df["source_file"]
        staging_df["source_version"] = pd.to_numeric(df["source_version"], errors="coerce").fillna(1).astype(int)
        staging_df["run_id"] = run_id

        staging_df.to_sql(
            "raw_transaction_events",
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=Config.BATCH_SIZE
        )
        logger.info(f"Staged {len(staging_df)} raw records into raw_transaction_events for run {run_id}.")

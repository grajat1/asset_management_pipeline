"""Shared configuration, database connection and input parsing helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SCHEMA_VERSION = "1.2"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
LOGGER = logging.getLogger("asset_management_pipeline")

INPUTS = {
    "security_master": ("security_master.csv", "SECURITY_MASTER_RAW"),
    "transactions": ("transactions.csv", "TRANSACTIONS_RAW"),
    "positions": ("positions.csv", "POSITIONS_RAW"),
    "market_prices": ("market_prices.csv", "MARKET_PRICES_RAW"),
    "restricted_securities": ("restricted_securities.csv", "RESTRICTED_SECURITIES_RAW"),
}

EXPECTED_COLUMNS = {
    "security_master": {"security_id", "isin", "asset_class", "currency", "effective_from", "effective_to"},
    "transactions": {"transaction_id", "portfolio_id", "security_id", "trade_date", "quantity", "price", "currency"},
    "positions": {"portfolio_id", "security_id", "position_date", "quantity", "book_value", "currency"},
    "market_prices": {"security_id", "price_date", "price", "price_currency", "price_source"},
    "restricted_securities": {"security_id", "restriction_type", "effective_from", "effective_to", "status"},
}

OPTIONAL_COLUMNS = {
    "security_master": set(),
    "transactions": {"record_version", "supersedes_version", "correction_reason"},
    "positions": set(),
    "market_prices": set(),
    "restricted_securities": set(),
}

def connect() -> oracledb.Connection:
    """Connect using environment variables; never hard-code secrets."""
    required = ("DB_USER", "DB_PASSWORD", "DB_DSN")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError(
            "Missing environment variables: " + ", ".join(missing)
        )
    # Import here so pure parsing/unit tests can run without Oracle installed.
    import oracledb

    return oracledb.connect(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dsn=os.environ["DB_DSN"],
    )

def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def delivery_fingerprint(business_date: date, dataset_hashes: dict[str, tuple[str, str]], schema_version: str = SCHEMA_VERSION) -> str:
    """Hash a deterministic dataset-aware delivery identity.

    dataset_hashes maps dataset name to (file name, SHA-256 content hash).
    """
    payload = {
        "business_date": business_date.isoformat(),
        "schema_version": schema_version,
        "datasets": {
            name: {"file_name": file_name, "sha256": digest}
            for name, (file_name, digest) in sorted(dataset_hashes.items())
        },
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def parse_date(value: str | None) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        # Invalid source dates become null and are quarantined by validation.
        return None

def parse_decimal(value: str | None) -> Decimal | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        parsed = Decimal(value)
        return parsed if parsed.is_finite() else None
    except InvalidOperation:
        return None

def parse_version(value: str | None, default: int = 1) -> int:
    """Parse a transaction version; invalid supplied values become 0 for quarantine."""
    value = (value or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return 0

def read_csv(path: Path, dataset: str) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError(f"{path.name}: missing CSV header")
        normalized_headers = [name.strip().lower() for name in reader.fieldnames]
        if len(normalized_headers) != len(set(normalized_headers)):
            raise ValueError(f"{path.name}: duplicate CSV headers are not allowed")
        actual = set(normalized_headers)
        expected = EXPECTED_COLUMNS[dataset]
        missing = expected - actual
        unexpected = actual - expected - OPTIONAL_COLUMNS[dataset]
        if missing:
            raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
        if unexpected:
            raise ValueError(
                f"{path.name}: unsupported columns {sorted(unexpected)}; "
                "review schema evolution before accepting this delivery"
            )
        # Normalize keys to lowercase/trimmed names.
        normalized_rows: list[dict[str, str]] = []
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise ValueError(
                    f"{path.name}: row {row_number} has more fields than the header"
                )
            normalized_rows.append(
                {str(key).strip().lower(): (value.strip() if isinstance(value, str) else "")
                 for key, value in row.items()}
            )
        return normalized_rows

def insert_run(cursor: Any, business_date: date) -> int:
    import oracledb
    run_id_var = cursor.var(oracledb.NUMBER)
    cursor.execute(
        """
        INSERT INTO PIPELINE_RUNS
          (BUSINESS_DATE, STATUS, STARTED_AT)
        VALUES (:business_date, 'RUNNING', SYSTIMESTAMP)
        RETURNING RUN_ID INTO :run_id
        """,
        {"business_date": business_date, "run_id": run_id_var},
    )
    return int(run_id_var.getvalue())

def register_delivery(
    cursor: Any, file_hash: str, file_name: str, run_id: int,
    force_replay: bool = False,
) -> bool:
    """Register a delivery attempt; optionally bypass successful-delivery dedupe."""
    cursor.execute(
        "SELECT STATUS FROM FILE_DELIVERIES WHERE FILE_HASH = :file_hash",
        {"file_hash": file_hash},
    )
    existing = cursor.fetchone()
    if existing and existing[0] == "SUCCEEDED" and not force_replay:
        cursor.execute(
            "INSERT INTO FILE_DELIVERY_ATTEMPTS (RUN_ID, FILE_HASH, ATTEMPT_STATUS, COMPLETED_AT) "
            "VALUES (:run_id, :file_hash, 'SKIPPED_DUPLICATE', SYSTIMESTAMP)",
            {"run_id": run_id, "file_hash": file_hash},
        )
        return False
    if existing:
        cursor.execute(
            """
            UPDATE FILE_DELIVERIES
            SET RUN_ID = :run_id, STATUS = 'PROCESSING',
                RECEIVED_AT = SYSTIMESTAMP, COMPLETED_AT = NULL
            WHERE FILE_HASH = :file_hash
            """,
            {"run_id": run_id, "file_hash": file_hash},
        )
    else:
        cursor.execute(
            """
            INSERT INTO FILE_DELIVERIES
              (FILE_HASH, FILE_NAME, RUN_ID, STATUS)
            VALUES (:file_hash, :file_name, :run_id, 'PROCESSING')
            """,
            {
                "file_hash": file_hash,
                "file_name": file_name,
                "run_id": run_id,
            },
        )
    cursor.execute(
        "INSERT INTO FILE_DELIVERY_ATTEMPTS (RUN_ID, FILE_HASH, ATTEMPT_STATUS) "
        "VALUES (:run_id, :file_hash, 'PROCESSING')",
        {"run_id": run_id, "file_hash": file_hash},
    )
    return True

def raw_insert_sql(table: str) -> str:
    columns = {
        "SECURITY_MASTER_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, ISIN, "
            "ASSET_CLASS, CURRENCY, EFFECTIVE_FROM, EFFECTIVE_TO"
        ),
        "TRANSACTIONS_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, "
            "PORTFOLIO_ID, SECURITY_ID, TRADE_DATE, QUANTITY, PRICE, CURRENCY, "
            "RECORD_VERSION, SUPERSEDES_VERSION, CORRECTION_REASON"
        ),
        "POSITIONS_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, PORTFOLIO_ID, SECURITY_ID, "
            "POSITION_DATE, QUANTITY, BOOK_VALUE, CURRENCY"
        ),
        "MARKET_PRICES_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, PRICE_DATE, "
            "PRICE, PRICE_CURRENCY, PRICE_SOURCE"
        ),
        "RESTRICTED_SECURITIES_RAW": (
            "RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, SECURITY_ID, "
            "RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO, STATUS"
        ),
    }
    if table not in columns:
        raise ValueError("Unsupported raw table")
    names = columns[table].split(", ")
    binds = ", ".join(f":{i}" for i in range(1, len(names) + 1))
    return f"INSERT INTO {table} ({columns[table]}) VALUES ({binds})"

def raw_values(
    dataset: str, row: dict[str, str], run_id: int, source_file: str, row_num: int
) -> tuple[Any, ...]:
    prefix = (run_id, source_file, row_num)
    if dataset == "security_master":
        return prefix + (
            row.get("security_id") or None, row.get("isin") or None,
            row.get("asset_class") or None, (row.get("currency") or "").upper() or None,
            parse_date(row.get("effective_from")), parse_date(row.get("effective_to")),
        )
    if dataset == "transactions":
        return prefix + (
            row.get("transaction_id") or None, row.get("portfolio_id") or None,
            row.get("security_id") or None, parse_date(row.get("trade_date")),
            parse_decimal(row.get("quantity")), parse_decimal(row.get("price")),
            (row.get("currency") or "").upper() or None, parse_version(row.get("record_version")),
            parse_version(row.get("supersedes_version"), default=0) or None,
            row.get("correction_reason") or None,
        )
    if dataset == "positions":
        return prefix + (
            row.get("portfolio_id") or None, row.get("security_id") or None,
            parse_date(row.get("position_date")), parse_decimal(row.get("quantity")),
            parse_decimal(row.get("book_value")), (row.get("currency") or "").upper() or None,
        )
    if dataset == "market_prices":
        return prefix + (
            row.get("security_id") or None, parse_date(row.get("price_date")),
            parse_decimal(row.get("price")), (row.get("price_currency") or "").upper() or None,
            row.get("price_source") or None,
        )
    if dataset == "restricted_securities":
        return prefix + (
            row.get("security_id") or None, row.get("restriction_type") or None,
            parse_date(row.get("effective_from")), parse_date(row.get("effective_to")),
            row.get("status") or None,
        )
    raise ValueError(f"Unknown dataset {dataset}")

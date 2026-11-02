"""Transaction validation, correction versioning and canonical merge."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from typing import Any


def _canonical_decimal(value: Any) -> str:
    """Return a scale-independent decimal representation for hashing."""
    number = value if isinstance(value, Decimal) else Decimal(str(value))
    if number == 0:
        return "0"
    return format(number.normalize(), "f")

def transaction_record_hash(
    tx_id: str, portfolio: str, security: str, trade_day: date, qty: Any,
    price: Any, currency: str, record_version: int,
    supersedes_version: int | None, correction_reason: str | None,
) -> str:
    """Hash canonical business fields (not source formatting or row location)."""
    payload = {
        "transaction_id": tx_id,
        "portfolio_id": portfolio,
        "security_id": security,
        "trade_date": trade_day.isoformat(),
        "quantity": _canonical_decimal(qty),
        "price": _canonical_decimal(price),
        "currency": currency.strip().upper(),
        "record_version": int(record_version),
        "supersedes_version": supersedes_version,
        "correction_reason": (correction_reason or "").strip() or None,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def validate_version_transition(
    incoming_version: int, supersedes_version: int | None, current_version: int | None
) -> str | None:
    """Validate a consecutive, explicitly linked transaction correction."""
    if current_version is None:
        return None if incoming_version == 1 and supersedes_version is None else "CORRECTION_BASE_VERSION_NOT_FOUND"
    if incoming_version < current_version:
        return "STALE_TRANSACTION_VERSION"
    if incoming_version == current_version:
        return None
    if incoming_version != current_version + 1:
        return "NON_CONSECUTIVE_TRANSACTION_VERSION"
    if supersedes_version != current_version:
        return "INVALID_SUPERSEDES_VERSION"
    return None

def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def _security_currency_on_date(
    security_history: dict[str, list[tuple[date, date | None, str]]],
    security_id: str, effective_date: date,
) -> str | None:
    """Resolve the unique effective-dated security currency from cached history."""
    matches = [
        currency for start, end, currency in security_history.get(security_id, [])
        if effective_date >= start and (end is None or effective_date <= end)
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def process_transactions(
    cursor: Any, run_id: int, business_date: date
) -> tuple[int, int, int, int]:
    """Validate raw transaction rows and merge accepted canonical records."""
    cursor.execute(
        """
        SELECT SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, PORTFOLIO_ID,
               SECURITY_ID, TRADE_DATE, QUANTITY, PRICE, CURRENCY,
               NVL(RECORD_VERSION, 1), SUPERSEDES_VERSION, CORRECTION_REASON
        FROM TRANSACTIONS_RAW
        WHERE RUN_ID = :run_id
        ORDER BY SOURCE_FILE, SOURCE_ROW_NUMBER
        """,
        {"run_id": run_id},
    )
    raw_rows = cursor.fetchall()
    received = len(raw_rows)

    # Load effective-dated Security Master once rather than issuing one query per
    # transaction. This avoids an N+1 database round-trip pattern at scale.
    security_ids = sorted({row[4] for row in raw_rows if row[4]})
    security_history: dict[str, list[tuple[date, date | None, str]]] = {}
    for offset in range(0, len(security_ids), 500):
        batch = security_ids[offset:offset + 500]
        binds = {f"security_{index}": value for index, value in enumerate(batch)}
        placeholders = ", ".join(f":security_{index}" for index in range(len(batch)))
        cursor.execute(
            "SELECT SECURITY_ID, EFFECTIVE_FROM, EFFECTIVE_TO, CURRENCY "
            f"FROM SECURITY_MASTER WHERE SECURITY_ID IN ({placeholders})",
            binds,
        )
        for security_id, effective_from, effective_to, security_currency in cursor.fetchall():
            start_day, end_day = _as_date(effective_from), _as_date(effective_to)
            if start_day is not None and security_currency:
                security_history.setdefault(security_id, []).append(
                    (start_day, end_day, str(security_currency).upper())
                )

    valid_by_id: dict[tuple[str, int], tuple[Any, ...]] = {}
    rejects: list[tuple[Any, ...]] = []
    duplicates = 0

    for (source_file, row_num, tx_id, portfolio, security, trade_date, qty, price,
         currency, record_version, supersedes_version, correction_reason) in raw_rows:
        reason = None
        if not tx_id:
            reason = "MISSING_TRANSACTION_ID"
        elif not portfolio:
            reason = "MISSING_PORTFOLIO_ID"
        elif not security:
            reason = "MISSING_SECURITY_ID"
        elif trade_date is None:
            reason = "INVALID_OR_MISSING_TRADE_DATE"
        elif qty is None or qty == 0:
            reason = "INVALID_QUANTITY"
        elif price is None or price <= 0:
            reason = "INVALID_PRICE"
        elif not currency:
            reason = "MISSING_CURRENCY"
        elif record_version is None or record_version < 1 or int(record_version) != record_version:
            reason = "INVALID_RECORD_VERSION"
        elif supersedes_version is not None and int(supersedes_version) != supersedes_version:
            reason = "INVALID_SUPERSEDES_VERSION"
        elif record_version == 1 and supersedes_version is not None:
            reason = "INVALID_INITIAL_VERSION_METADATA"
        elif record_version > 1 and (supersedes_version is None or not correction_reason):
            reason = "CORRECTION_METADATA_REQUIRED"

        trade_day = _as_date(trade_date)
        if reason is None and trade_day is None:
            reason = "INVALID_OR_MISSING_TRADE_DATE"
        if reason is None and trade_day > business_date:
            reason = "FUTURE_TRADE_DATE"

        if reason is None:
            security_currency = _security_currency_on_date(
                security_history, security, trade_day
            )
            if security_currency is None:
                reason = "UNKNOWN_OR_INACTIVE_SECURITY"
            elif security_currency != currency.upper():
                reason = "TRANSACTION_CURRENCY_MISMATCH"

        if reason:
            rejects.append((run_id, source_file, row_num, tx_id, reason, None))
            continue

        normalized = (
            tx_id, portfolio, security, trade_day, qty, price, currency,
            record_version, supersedes_version, correction_reason, source_file, row_num
        )
        version_key = (tx_id, record_version)
        prior = valid_by_id.get(version_key)
        if prior is not None:
            if prior[:10] != normalized[:10]:
                rejects.append((
                    run_id, source_file, row_num, tx_id,
                    "CONFLICTING_DUPLICATE_TRANSACTION",
                    "Same transaction ID has different business fields",
                ))
            else:
                duplicates += 1
            # Identical duplicate: keep first canonical copy only.
            continue
        valid_by_id[version_key] = normalized

    if rejects:
        cursor.executemany(
            """
            INSERT INTO TRANSACTION_REJECTS
              (RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, REASON, DETAILS)
            VALUES (:1, :2, :3, :4, :5, :6)
            """,
            rejects,
        )

    # Prefetch existing transaction history in bounded IN-clause batches. This
    # avoids two database round trips per incoming transaction while respecting
    # Oracle's practical bind-count limit.
    transaction_ids = sorted({row[0] for row in valid_by_id.values()})
    existing_versions: dict[str, dict[int, tuple[str, str]]] = {}
    current_by_id: dict[str, tuple[int, str]] = {}
    for offset in range(0, len(transaction_ids), 500):
        batch = transaction_ids[offset:offset + 500]
        binds = {f"tx_{index}": value for index, value in enumerate(batch)}
        placeholders = ", ".join(f":tx_{index}" for index in range(len(batch)))
        cursor.execute(
            "SELECT TRANSACTION_ID, RECORD_VERSION, RECORD_HASH, VERSION_STATUS "
            f"FROM TRANSACTIONS WHERE TRANSACTION_ID IN ({placeholders})",
            binds,
        )
        for tx_id, version, record_hash, status in cursor.fetchall():
            version_map = existing_versions.setdefault(tx_id, {})
            version_map[int(version)] = (record_hash, status)
            if status == "CURRENT":
                current_by_id[tx_id] = (int(version), record_hash)

    def record_reject(reject: tuple[Any, ...]) -> None:
        rejects.append(reject)
        cursor.execute(
            "INSERT INTO TRANSACTION_REJECTS "
            "(RUN_ID, SOURCE_FILE, SOURCE_ROW_NUMBER, TRANSACTION_ID, REASON, DETAILS) "
            "VALUES (:1, :2, :3, :4, :5, :6)",
            reject,
        )

    accepted_new = 0
    for row in sorted(valid_by_id.values(), key=lambda item: (item[0], item[7])):
        (tx_id, portfolio, security, trade_day, qty, price, currency,
         record_version, supersedes_version, correction_reason, source_file, row_num) = row
        record_hash = transaction_record_hash(
            tx_id, portfolio, security, trade_day, qty, price, currency,
            record_version, supersedes_version, correction_reason,
        )
        historical = existing_versions.get(tx_id, {}).get(record_version)
        if historical is not None:
            if historical[0] == record_hash:
                duplicates += 1
            else:
                record_reject((run_id, source_file, row_num, tx_id,
                               "CONFLICTING_DUPLICATE_TRANSACTION",
                               "Same transaction ID/version already exists with different business fields"))
            continue

        current = current_by_id.get(tx_id)
        current_version = current[0] if current else None
        if current is None and existing_versions.get(tx_id):
            transition_error = "TRANSACTION_VERSION_STATE_INVALID"
        else:
            transition_error = validate_version_transition(
                record_version, supersedes_version, current_version
            )
        if transition_error:
            record_reject((run_id, source_file, row_num, tx_id, transition_error,
                           f"Incoming version {record_version}; current version {current_version}"))
            continue

        if current is not None:
            cursor.execute(
                "UPDATE TRANSACTIONS SET VERSION_STATUS = 'SUPERSEDED' "
                "WHERE TRANSACTION_ID = :tx_id AND RECORD_VERSION = :version",
                {"tx_id": tx_id, "version": current_version},
            )
            existing_versions[tx_id][current_version] = (current[1], "SUPERSEDED")

        cursor.execute(
            """
            INSERT INTO TRANSACTIONS (
              TRANSACTION_ID, PORTFOLIO_ID, SECURITY_ID, TRADE_DATE,
              QUANTITY, PRICE, CURRENCY, SOURCE_FILE, SOURCE_ROW_NUMBER,
              RUN_ID, RECORD_HASH, RECORD_VERSION, VERSION_STATUS,
              SUPERSEDES_VERSION, CORRECTION_REASON
            ) VALUES (
              :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, 'CURRENT', :13, :14
            )
            """,
            (tx_id, portfolio, security, trade_day, qty, price, currency,
             source_file, row_num, run_id, record_hash, record_version,
             supersedes_version, correction_reason),
        )
        existing_versions.setdefault(tx_id, {})[record_version] = (record_hash, "CURRENT")
        current_by_id[tx_id] = (record_version, record_hash)
        accepted_new += 1

    return received, accepted_new, len(rejects), duplicates

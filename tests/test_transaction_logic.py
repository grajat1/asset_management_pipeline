"""Focused tests for transaction correction and temporal reference logic."""

import unittest
from datetime import date
from decimal import Decimal

from src.transaction_processor import (
    process_transactions,
    _security_currency_on_date,
    transaction_record_hash,
    validate_version_transition,
)


class FakeTransactionCursor:
    def __init__(self, raw_rows, security_rows):
        self.raw_rows = raw_rows
        self.security_rows = security_rows
        self.last_sql = ""
        self.executed = []

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.executed.append((sql, params))

    def fetchall(self):
        if "FROM TRANSACTIONS_RAW" in self.last_sql:
            return self.raw_rows
        if "FROM SECURITY_MASTER" in self.last_sql:
            return self.security_rows
        return []

    def fetchone(self):
        if "SELECT RECORD_HASH FROM TRANSACTIONS" in self.last_sql:
            return None
        if "SELECT RECORD_VERSION, RECORD_HASH FROM TRANSACTIONS" in self.last_sql:
            return None
        return None

    def executemany(self, sql, rows):
        self.executed.append((sql, rows))


class TestTransactionLogic(unittest.TestCase):
    def test_hash_is_independent_of_decimal_scale(self):
        common = ("T1", "P1", "S1", date(2026, 10, 29))
        first = transaction_record_hash(
            *common, Decimal("10.0"), Decimal("2.500"), "usd", 1, None, None
        )
        second = transaction_record_hash(
            *common, Decimal("10.00"), Decimal("2.5"), "USD", 1, None, None
        )
        self.assertEqual(first, second)

    def test_hash_changes_when_business_value_changes(self):
        common = ("T1", "P1", "S1", date(2026, 10, 29))
        first = transaction_record_hash(
            *common, Decimal("10"), Decimal("2.5"), "USD", 1, None, None
        )
        changed = transaction_record_hash(
            *common, Decimal("11"), Decimal("2.5"), "USD", 1, None, None
        )
        self.assertNotEqual(first, changed)

    def test_initial_transaction_requires_version_one(self):
        self.assertIsNone(validate_version_transition(1, None, None))
        self.assertEqual(
            validate_version_transition(2, 1, None),
            "CORRECTION_BASE_VERSION_NOT_FOUND",
        )

    def test_correction_must_be_consecutive_and_supersede_current(self):
        self.assertIsNone(validate_version_transition(2, 1, 1))
        self.assertEqual(
            validate_version_transition(5, 1, 1),
            "NON_CONSECUTIVE_TRANSACTION_VERSION",
        )
        self.assertEqual(
            validate_version_transition(2, None, 1),
            "INVALID_SUPERSEDES_VERSION",
        )
        self.assertEqual(
            validate_version_transition(1, None, 2),
            "STALE_TRANSACTION_VERSION",
        )

    def test_process_transactions_accepts_datetime_trade_date(self):
        from datetime import datetime
        from decimal import Decimal

        raw_rows = [(
            "transactions.csv", 1, "T100", "P1", "S1",
            datetime(2026, 10, 29, 0, 0), Decimal("10.00"),
            Decimal("2.50"), "USD", 1, None, None,
        )]
        security_rows = [("S1", date(2026, 1, 1), None, "USD")]
        cursor = FakeTransactionCursor(raw_rows, security_rows)
        result = process_transactions(cursor, 42, date(2026, 10, 29))
        self.assertEqual(result, (1, 1, 0, 0))
        self.assertTrue(any("INSERT INTO TRANSACTIONS" in sql for sql, _ in cursor.executed))

    def test_process_transactions_rejects_currency_mismatch(self):
        from datetime import datetime
        from decimal import Decimal

        raw_rows = [(
            "transactions.csv", 2, "T101", "P1", "S1",
            datetime(2026, 10, 29), Decimal("10"), Decimal("2.5"),
            "EUR", 1, None, None,
        )]
        cursor = FakeTransactionCursor(
            raw_rows, [("S1", date(2026, 1, 1), None, "USD")]
        )
        result = process_transactions(cursor, 43, date(2026, 10, 29))
        self.assertEqual(result, (1, 0, 1, 0))
        self.assertTrue(any("TRANSACTION_REJECTS" in sql for sql, _ in cursor.executed))

    def test_security_currency_lookup_respects_effective_dates(self):
        history = {
            "S1": [
                (date(2025, 1, 1), date(2025, 12, 31), "USD"),
                (date(2026, 1, 1), None, "EUR"),
            ]
        }
        self.assertEqual(
            _security_currency_on_date(history, "S1", date(2025, 6, 1)), "USD"
        )
        self.assertEqual(
            _security_currency_on_date(history, "S1", date(2026, 6, 1)), "EUR"
        )
        self.assertIsNone(
            _security_currency_on_date(history, "S1", date(2024, 12, 31))
        )
        self.assertIsNone(
            _security_currency_on_date(history, "UNKNOWN", date(2026, 6, 1))
        )

    def test_security_currency_lookup_rejects_ambiguous_overlap(self):
        history = {
            "S1": [
                (date(2025, 1, 1), date(2026, 6, 30), "USD"),
                (date(2026, 1, 1), None, "EUR"),
            ]
        }
        self.assertIsNone(
            _security_currency_on_date(history, "S1", date(2026, 3, 1))
        )


if __name__ == "__main__":
    unittest.main()

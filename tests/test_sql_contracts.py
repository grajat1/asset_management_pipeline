"""SQL-generation contract tests; live Oracle execution remains an integration test."""

import unittest
from datetime import date
from pathlib import Path

from src.gold_processor import build_gold
from src.reference_loader import load_reference_and_raw


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)

    def fetchone(self):
        return (0,)


class TestSqlContracts(unittest.TestCase):
    def test_reference_loader_quarantines_and_filters_invalid_rows(self):
        cursor = RecordingCursor()
        load_reference_and_raw(cursor, 9001, {})
        sql = "\n".join(cursor.statements).upper()
        self.assertIn("INSERT INTO REFERENCE_DATA_REJECTS", sql)
        self.assertIn("UNKNOWN_OR_INACTIVE_SECURITY", sql)
        self.assertIn("POSITION_CURRENCY_MISMATCH", sql)
        self.assertIn("PARTITION BY SECURITY_ID, RESTRICTION_TYPE", sql)
        self.assertIn("NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS", sql)
        self.assertIn("N.EFFECTIVE_FROM=R.EFFECTIVE_TO+1", sql)
        self.assertIn("EFFECTIVE_FROM=SM.EFFECTIVE_FROM", sql)

    def test_gold_sql_has_explicit_security_price_and_currency_exceptions(self):
        cursor = RecordingCursor()
        build_gold(cursor, 9002, date(2026, 10, 29))
        sql = "\n".join(cursor.statements).upper()
        self.assertIn("UNKNOWN_OR_INACTIVE_SECURITY", sql)
        self.assertIn("MISSING_MARKET_PRICE", sql)
        self.assertIn("INVALID_MARKET_PRICE", sql)
        self.assertIn("STALE_MARKET_PRICE", sql)
        self.assertIn("POSITION_CURRENCY_MISMATCH", sql)
        self.assertIn("TRANSACTION_CURRENCY_MISMATCH", sql)
        self.assertIn("LEFT JOIN SECURITY_MASTER", sql)
        self.assertIn("T.VERSION_STATUS = 'CURRENT'", sql)

    def test_reference_gold_sql_script_matches_expected_statements(self):
        root = Path(__file__).resolve().parents[1]
        sql = (root / "sql" / "03_process_run.sql").read_text().upper()
        self.assertIn("INSERT INTO GOLD_DATA_QUALITY_EXCEPTIONS", sql)
        self.assertIn("INSERT INTO GOLD_PORTFOLIO_POSITIONS", sql)
        self.assertIn("INSERT INTO GOLD_COMPLIANCE_EXCEPTIONS", sql)
        self.assertIn("UNKNOWN_OR_INACTIVE_SECURITY", sql)
        self.assertIn("POSITION_CURRENCY_MISMATCH", sql)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for parsing, delivery identity and CSV contract validation."""

import tempfile
import unittest
from datetime import date
from pathlib import Path

from src.pipeline import (
    delivery_fingerprint, file_sha256, parse_date, parse_decimal, parse_version, raw_values, read_csv,
)


class TestHelpers(unittest.TestCase):
    def test_parse_date_valid_and_blank(self):
        self.assertEqual(parse_date("2026-10-29"), date(2026, 10, 29))
        self.assertIsNone(parse_date(""))

    def test_invalid_date_is_quarantinable(self):
        self.assertIsNone(parse_date("2026-02-30"))
        self.assertIsNone(parse_date("not-a-date"))

    def test_parse_transaction_version(self):
        self.assertEqual(parse_version(""), 1)
        self.assertEqual(parse_version("2"), 2)
        self.assertEqual(parse_version("invalid"), 0)

    def test_parse_decimal(self):
        from decimal import Decimal
        self.assertEqual(parse_decimal("12.50"), Decimal("12.50"))
        self.assertIsNone(parse_decimal(""))
        self.assertIsNone(parse_decimal("not-a-number"))
        self.assertIsNone(parse_decimal("NaN"))
        self.assertIsNone(parse_decimal("Infinity"))

    def test_currency_values_are_normalized_to_uppercase(self):
        values = raw_values(
            "transactions",
            {"transaction_id": "T1", "portfolio_id": "P1", "security_id": "S1",
             "trade_date": "2026-10-29", "quantity": "1", "price": "2",
             "currency": "usd"},
            1, "transactions.csv", 1,
        )
        self.assertEqual(values[9], "USD")

    def test_file_hash_is_repeatable_and_content_sensitive(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            path.write_text("a,b\n1,2\n", encoding="utf-8")
            first = file_sha256(path)
            self.assertEqual(first, file_sha256(path))
            path.write_text("a,b\n1,3\n", encoding="utf-8")
            self.assertNotEqual(first, file_sha256(path))

    def test_delivery_fingerprint_is_deterministic_and_date_aware(self):
        files = {
            "transactions": ("tx.csv", "a" * 64),
            "positions": ("pos.csv", "b" * 64),
        }
        baseline = delivery_fingerprint(date(2026, 10, 29), files)
        self.assertEqual(baseline, delivery_fingerprint(date(2026, 10, 29), dict(reversed(list(files.items())))))
        self.assertNotEqual(baseline, delivery_fingerprint(date(2026, 10, 30), files))
        changed = dict(files)
        changed["transactions"] = ("tx.csv", "c" * 64)
        self.assertNotEqual(baseline, delivery_fingerprint(date(2026, 10, 29), changed))

    def test_transaction_csv_has_required_columns(self):
        path = Path(__file__).resolve().parents[1] / "data" / "transactions.csv"
        rows = read_csv(path, "transactions")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["transaction_id"], "T1001")

    def test_duplicate_headers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text(
                "transaction_id,transaction_id,portfolio_id,security_id,trade_date,quantity,price,currency\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate CSV headers"):
                read_csv(path, "transactions")



    def test_csv_row_with_extra_field_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text(
                "transaction_id,portfolio_id,security_id,trade_date,quantity,price,currency\n"
                "T1,P1,S1,2026-10-29,1,10,USD,EXTRA\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "more fields than the header"):
                read_csv(path, "transactions")

    def test_unrecognized_schema_column_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text(
                "transaction_id,portfolio_id,security_id,trade_date,quantity,price,currency,unexpected_field\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "unsupported columns"):
                read_csv(path, "transactions")

    def test_missing_required_column_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "transactions.csv"
            path.write_text("transaction_id,portfolio_id\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing columns"):
                read_csv(path, "transactions")


if __name__ == "__main__":
    unittest.main()

"""Generate synthetic transaction CSV files for a simple volume test."""

import argparse
import csv
from pathlib import Path


def generate(rows: int, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow([
            "transaction_id", "portfolio_id", "security_id",
            "trade_date", "quantity", "price", "currency"
        ])
        for i in range(1, rows + 1):
            writer.writerow([
                f"TV{i:09d}",
                f"P{(i % 100) + 1:03d}",
                f"S{(i % 3) + 101}",
                "2026-10-29",
                10 if i % 2 else -10,
                50 if i % 3 else 102,
                "USD",
            ])
    print(f"Generated {rows} synthetic transactions: {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output", type=Path, default=Path("data/transactions_large.csv"))
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("--rows must be positive")
    generate(args.rows, args.output)


if __name__ == "__main__":
    main()

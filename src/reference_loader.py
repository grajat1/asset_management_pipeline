"""Bronze ingestion and curated reference-data validation/merge."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .pipeline_common import DATA_DIR, INPUTS, raw_insert_sql, raw_values, read_csv


def _effective_security_master_sql() -> str:
    """Return the incoming-over-existing effective-dated Security Master view."""
    return """SELECT SM.SECURITY_ID, SM.CURRENCY, SM.EFFECTIVE_FROM, SM.EFFECTIVE_TO
      FROM SECURITY_MASTER SM
      WHERE NOT EXISTS (
        SELECT 1 FROM SECURITY_MASTER_RAW R
        WHERE R.RUN_ID=:run_id AND R.SECURITY_ID=SM.SECURITY_ID
          AND R.EFFECTIVE_FROM=SM.EFFECTIVE_FROM
          AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
            WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='security_master'
              AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)
      )
      UNION
      SELECT R.SECURITY_ID, R.CURRENCY, R.EFFECTIVE_FROM, R.EFFECTIVE_TO
      FROM SECURITY_MASTER_RAW R
      WHERE R.RUN_ID=:run_id AND R.SECURITY_ID IS NOT NULL
        AND R.EFFECTIVE_FROM IS NOT NULL AND R.CURRENCY IS NOT NULL
        AND (R.EFFECTIVE_TO IS NULL OR R.EFFECTIVE_TO >= R.EFFECTIVE_FROM)
        AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
          WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='security_master'
            AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)"""


def _quarantine_invalid_reference_rows(cursor: Any, run_id: int) -> None:
    """Record invalid reference rows and exclude them from curated merges."""
    checks = [
        ("security_master", "SECURITY_MASTER_RAW",
         "SECURITY_ID IS NULL OR EFFECTIVE_FROM IS NULL OR CURRENCY IS NULL OR (EFFECTIVE_TO IS NOT NULL AND EFFECTIVE_TO < EFFECTIVE_FROM)",
         "INVALID_SECURITY_MASTER_FIELDS",
         "R.SECURITY_ID || ':' || TO_CHAR(R.EFFECTIVE_FROM, 'YYYY-MM-DD')",
         "Required Security Master fields are missing or the effective interval is invalid"),
        ("positions", "POSITIONS_RAW",
         "PORTFOLIO_ID IS NULL OR SECURITY_ID IS NULL OR POSITION_DATE IS NULL OR QUANTITY IS NULL OR BOOK_VALUE IS NULL OR CURRENCY IS NULL",
         "INVALID_POSITION_FIELDS",
         "R.PORTFOLIO_ID || ':' || R.SECURITY_ID || ':' || TO_CHAR(R.POSITION_DATE, 'YYYY-MM-DD')",
         "Required position fields are missing or non-numeric"),
        ("market_prices", "MARKET_PRICES_RAW",
         "SECURITY_ID IS NULL OR PRICE_DATE IS NULL OR PRICE IS NULL OR PRICE <= 0 OR PRICE_CURRENCY IS NULL OR PRICE_SOURCE IS NULL",
         "INVALID_MARKET_PRICE_FIELDS",
         "R.SECURITY_ID || ':' || TO_CHAR(R.PRICE_DATE, 'YYYY-MM-DD') || ':' || R.PRICE_SOURCE",
         "Required price fields are missing or the price is not positive"),
        ("restricted_securities", "RESTRICTED_SECURITIES_RAW",
         "SECURITY_ID IS NULL OR RESTRICTION_TYPE IS NULL OR EFFECTIVE_FROM IS NULL OR STATUS IS NULL OR (EFFECTIVE_TO IS NOT NULL AND EFFECTIVE_TO < EFFECTIVE_FROM)",
         "INVALID_RESTRICTION_FIELDS",
         "R.SECURITY_ID || ':' || R.RESTRICTION_TYPE || ':' || TO_CHAR(R.EFFECTIVE_FROM, 'YYYY-MM-DD')",
         "Required restriction fields are missing or the effective interval is invalid"),
    ]
    for dataset, table, predicate, reason, natural_key, details in checks:
        cursor.execute(
            f"""INSERT INTO REFERENCE_DATA_REJECTS
              (RUN_ID, DATASET_NAME, SOURCE_FILE, SOURCE_ROW_NUMBER, NATURAL_KEY, REASON, DETAILS)
              SELECT R.RUN_ID, :dataset, R.SOURCE_FILE, R.SOURCE_ROW_NUMBER,
                     {natural_key}, :reason, :details
              FROM {table} R
              WHERE R.RUN_ID = :run_id AND ({predicate})
                AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
                  WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME=:dataset
                    AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)""",
            {"run_id": run_id, "dataset": dataset, "reason": reason, "details": details},
        )

    # Cross-reference position, price and restriction records to effective Security Master history.
    for dataset, table, date_column in (
        ("positions", "POSITIONS_RAW", "POSITION_DATE"),
        ("market_prices", "MARKET_PRICES_RAW", "PRICE_DATE"),
        ("restricted_securities", "RESTRICTED_SECURITIES_RAW", "EFFECTIVE_FROM"),
    ):
        cursor.execute(
            f"""INSERT INTO REFERENCE_DATA_REJECTS
              (RUN_ID, DATASET_NAME, SOURCE_FILE, SOURCE_ROW_NUMBER, NATURAL_KEY, REASON, DETAILS)
              SELECT R.RUN_ID, :dataset, R.SOURCE_FILE, R.SOURCE_ROW_NUMBER,
                     R.SECURITY_ID, 'UNKNOWN_OR_INACTIVE_SECURITY',
                     'No effective Security Master row for the source date'
              FROM {table} R
              WHERE R.RUN_ID=:run_id AND R.SECURITY_ID IS NOT NULL
                AND R.{date_column} IS NOT NULL
                AND NOT EXISTS (
                  SELECT 1 FROM ({_effective_security_master_sql()}) SM
                  WHERE SM.SECURITY_ID=R.SECURITY_ID
                    AND R.{date_column} >= SM.EFFECTIVE_FROM
                    AND (SM.EFFECTIVE_TO IS NULL OR R.{date_column} <= SM.EFFECTIVE_TO)
                )
                AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
                  WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME=:dataset
                    AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)""",
            {"run_id": run_id, "dataset": dataset},
        )

    cursor.execute(
        f"""INSERT INTO REFERENCE_DATA_REJECTS
          (RUN_ID, DATASET_NAME, SOURCE_FILE, SOURCE_ROW_NUMBER, NATURAL_KEY, REASON, DETAILS)
          SELECT R.RUN_ID, 'positions', R.SOURCE_FILE, R.SOURCE_ROW_NUMBER,
                 R.PORTFOLIO_ID || ':' || R.SECURITY_ID || ':' || TO_CHAR(R.POSITION_DATE, 'YYYY-MM-DD'),
                 'POSITION_CURRENCY_MISMATCH',
                 'Position currency differs from effective Security Master currency'
          FROM POSITIONS_RAW R
          WHERE R.RUN_ID=:run_id AND EXISTS (
            SELECT 1 FROM ({_effective_security_master_sql()}) SM
            WHERE SM.SECURITY_ID=R.SECURITY_ID
              AND R.POSITION_DATE >= SM.EFFECTIVE_FROM
              AND (SM.EFFECTIVE_TO IS NULL OR R.POSITION_DATE <= SM.EFFECTIVE_TO)
              AND UPPER(SM.CURRENCY) <> UPPER(R.CURRENCY))
            AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
              WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='positions'
                AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)""",
        {"run_id": run_id},
    )


def _exclude_rejected(dataset: str) -> str:
    return ("AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q "
            "WHERE Q.RUN_ID=R.RUN_ID "
            f"AND Q.DATASET_NAME='{dataset}' "
            "AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)")


def load_reference_and_raw(
    cursor: Any, run_id: int, input_paths: dict[str, Path]
) -> dict[str, int]:
    """Load all five CSV files into Bronze and merge reference data."""
    counts: dict[str, int] = {}
    for dataset, (default_name, table) in INPUTS.items():
        path = input_paths.get(dataset, DATA_DIR / default_name)
        rows = read_csv(path, dataset)
        values = [
            raw_values(dataset, row, run_id, path.name, idx)
            for idx, row in enumerate(rows, start=1)
        ]
        if values:
            cursor.executemany(raw_insert_sql(table), values)
        counts[dataset] = len(values)

    # Quarantine invalid reference rows individually; retain raw Bronze lineage.
    _quarantine_invalid_reference_rows(cursor, run_id)

    # Do not silently choose an arbitrary row when the same natural key has
    # conflicting reference values. Exact duplicate rows remain harmless.
    conflict_checks = {
        "security_master": (
            "SELECT COUNT(*) FROM (SELECT SECURITY_ID, EFFECTIVE_FROM FROM SECURITY_MASTER_RAW R "
            "WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='security_master' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) GROUP BY SECURITY_ID, EFFECTIVE_FROM "
            "HAVING COUNT(DISTINCT RAWTOHEX(STANDARD_HASH(NVL(ISIN,'~') || '|' || NVL(ASSET_CLASS,'~') || '|' || NVL(CURRENCY,'~') || '|' || NVL(TO_CHAR(EFFECTIVE_TO,'YYYY-MM-DD'),'~'), 'SHA256'))) > 1)"
        ),
        "positions": (
            "SELECT COUNT(*) FROM (SELECT PORTFOLIO_ID, SECURITY_ID, POSITION_DATE FROM POSITIONS_RAW R "
            "WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='positions' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) GROUP BY PORTFOLIO_ID, SECURITY_ID, POSITION_DATE "
            "HAVING COUNT(DISTINCT RAWTOHEX(STANDARD_HASH(NVL(TO_CHAR(QUANTITY),'~') || '|' || NVL(TO_CHAR(BOOK_VALUE),'~') || '|' || NVL(CURRENCY,'~'), 'SHA256'))) > 1)"
        ),
        "market_prices": (
            "SELECT COUNT(*) FROM (SELECT SECURITY_ID, PRICE_DATE, PRICE_SOURCE FROM MARKET_PRICES_RAW R "
            "WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='market_prices' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) GROUP BY SECURITY_ID, PRICE_DATE, PRICE_SOURCE "
            "HAVING COUNT(DISTINCT RAWTOHEX(STANDARD_HASH(NVL(TO_CHAR(PRICE),'~') || '|' || NVL(PRICE_CURRENCY,'~'), 'SHA256'))) > 1)"
        ),
        "restricted_securities": (
            "SELECT COUNT(*) FROM (SELECT SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM FROM RESTRICTED_SECURITIES_RAW R "
            "WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='restricted_securities' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) GROUP BY SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM "
            "HAVING COUNT(DISTINCT RAWTOHEX(STANDARD_HASH(NVL(TO_CHAR(EFFECTIVE_TO,'YYYY-MM-DD'),'~') || '|' || NVL(STATUS,'~'), 'SHA256'))) > 1)"
        ),
    }
    for dataset, query in conflict_checks.items():
        cursor.execute(query, {"run_id": run_id})
        conflict_count = cursor.fetchone()[0]
        if conflict_count:
            raise ValueError(
                f"{dataset}: found {conflict_count} natural keys with conflicting reference values; "
                "delivery is not merged and must be corrected or explicitly versioned"
            )

    # Do not overwrite an already-curated historical natural key with a changed
    # payload. Corrections must arrive as a new effective/versioned record.
    existing_conflicts = {
        "security_master": (
            "SELECT COUNT(*) FROM SECURITY_MASTER T JOIN SECURITY_MASTER_RAW R "
            "ON T.SECURITY_ID = R.SECURITY_ID AND T.EFFECTIVE_FROM = R.EFFECTIVE_FROM "
            "WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='security_master' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) AND (NVL(T.ISIN,'~') <> NVL(R.ISIN,'~') "
            "OR NVL(T.ASSET_CLASS,'~') <> NVL(R.ASSET_CLASS,'~') "
            "OR NVL(T.CURRENCY,'~') <> NVL(R.CURRENCY,'~') "
            "OR (NVL(T.EFFECTIVE_TO, DATE '9999-12-31') <> NVL(R.EFFECTIVE_TO, DATE '9999-12-31') "
            "AND NOT (T.EFFECTIVE_TO IS NULL AND R.EFFECTIVE_TO IS NOT NULL AND EXISTS ("
            "SELECT 1 FROM SECURITY_MASTER_RAW N WHERE N.RUN_ID=R.RUN_ID "
            "AND N.SECURITY_ID=R.SECURITY_ID AND N.EFFECTIVE_FROM=R.EFFECTIVE_TO+1 "
            "AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=N.RUN_ID "
            "AND Q.DATASET_NAME='security_master' AND Q.SOURCE_ROW_NUMBER=N.SOURCE_ROW_NUMBER)))))"
        ),
        "positions": (
            "SELECT COUNT(*) FROM POSITIONS T JOIN POSITIONS_RAW R "
            "ON T.PORTFOLIO_ID = R.PORTFOLIO_ID AND T.SECURITY_ID = R.SECURITY_ID "
            "AND T.POSITION_DATE = R.POSITION_DATE WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='positions' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) "
            "AND (NVL(T.QUANTITY, -999999999) <> NVL(R.QUANTITY, -999999999) "
            "OR NVL(T.BOOK_VALUE, -999999999) <> NVL(R.BOOK_VALUE, -999999999) "
            "OR NVL(T.CURRENCY,'~') <> NVL(R.CURRENCY,'~'))"
        ),
        "market_prices": (
            "SELECT COUNT(*) FROM MARKET_PRICES T JOIN MARKET_PRICES_RAW R "
            "ON T.SECURITY_ID = R.SECURITY_ID AND T.PRICE_DATE = R.PRICE_DATE "
            "AND T.PRICE_SOURCE = R.PRICE_SOURCE WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='market_prices' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) "
            "AND (NVL(T.PRICE, -999999999) <> NVL(R.PRICE, -999999999) "
            "OR NVL(T.PRICE_CURRENCY,'~') <> NVL(R.PRICE_CURRENCY,'~'))"
        ),
        "restricted_securities": (
            "SELECT COUNT(*) FROM RESTRICTED_SECURITIES T JOIN RESTRICTED_SECURITIES_RAW R "
            "ON T.SECURITY_ID = R.SECURITY_ID AND T.RESTRICTION_TYPE = R.RESTRICTION_TYPE "
            "AND T.EFFECTIVE_FROM = R.EFFECTIVE_FROM WHERE R.RUN_ID = :run_id AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='restricted_securities' AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER) "
            "AND (NVL(T.EFFECTIVE_TO, DATE '9999-12-31') <> NVL(R.EFFECTIVE_TO, DATE '9999-12-31') "
            "OR NVL(T.STATUS,'~') <> NVL(R.STATUS,'~'))"
        ),
    }
    for dataset, query in existing_conflicts.items():
        cursor.execute(query, {"run_id": run_id})
        if cursor.fetchone()[0]:
            raise ValueError(
                f"{dataset}: source attempts to change an existing natural key; "
                "use a new effective-dated/versioned record or an approved correction workflow"
            )

    # Prevent overlapping Security Master effective intervals for a security.
    cursor.execute(
        f"""
        SELECT COUNT(*) FROM (
          SELECT SECURITY_ID, EFFECTIVE_FROM, EFFECTIVE_TO,
                 LEAD(EFFECTIVE_FROM) OVER (PARTITION BY SECURITY_ID ORDER BY EFFECTIVE_FROM) NEXT_FROM
          FROM ({_effective_security_master_sql()})
        ) WHERE NEXT_FROM IS NOT NULL
          AND (EFFECTIVE_TO IS NULL OR EFFECTIVE_TO >= NEXT_FROM)
        """,
        {"run_id": run_id},
    )
    if cursor.fetchone()[0]:
        raise ValueError("security_master: overlapping effective-date intervals detected")

    cursor.execute(
        """SELECT COUNT(*) FROM (
             SELECT SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO,
                    LEAD(EFFECTIVE_FROM) OVER (
                      PARTITION BY SECURITY_ID, RESTRICTION_TYPE ORDER BY EFFECTIVE_FROM
                    ) NEXT_FROM
             FROM (
               SELECT SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO
               FROM RESTRICTED_SECURITIES
               UNION
               SELECT R.SECURITY_ID, R.RESTRICTION_TYPE, R.EFFECTIVE_FROM, R.EFFECTIVE_TO
               FROM RESTRICTED_SECURITIES_RAW R
               WHERE R.RUN_ID=:run_id
                 AND NOT EXISTS (SELECT 1 FROM REFERENCE_DATA_REJECTS Q
                   WHERE Q.RUN_ID=R.RUN_ID AND Q.DATASET_NAME='restricted_securities'
                     AND Q.SOURCE_ROW_NUMBER=R.SOURCE_ROW_NUMBER)
             )
           ) WHERE NEXT_FROM IS NOT NULL
             AND (EFFECTIVE_TO IS NULL OR EFFECTIVE_TO >= NEXT_FROM)""",
        {"run_id": run_id},
    )
    if cursor.fetchone()[0]:
        raise ValueError("restricted_securities: overlapping restriction intervals detected")

    # Merge only accepted reference rows; rejected source rows remain in Bronze.
    # Each natural key is deterministic; effective-dated security versions are preserved.
    cursor.execute(f"""
        MERGE INTO SECURITY_MASTER T
        USING (
          SELECT SECURITY_ID, ISIN, ASSET_CLASS, CURRENCY,
                 EFFECTIVE_FROM, EFFECTIVE_TO
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID, EFFECTIVE_FROM
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM SECURITY_MASTER_RAW R
            WHERE RUN_ID = :run_id AND SECURITY_ID IS NOT NULL
              {_exclude_rejected("security_master")}
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID
            AND T.EFFECTIVE_FROM = S.EFFECTIVE_FROM)
        WHEN MATCHED THEN UPDATE SET
          T.ISIN = S.ISIN, T.ASSET_CLASS = S.ASSET_CLASS,
          T.CURRENCY = S.CURRENCY, T.EFFECTIVE_FROM = S.EFFECTIVE_FROM,
          T.EFFECTIVE_TO = S.EFFECTIVE_TO
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, ISIN, ASSET_CLASS, CURRENCY, EFFECTIVE_FROM, EFFECTIVE_TO)
          VALUES
          (S.SECURITY_ID, S.ISIN, S.ASSET_CLASS, S.CURRENCY,
           S.EFFECTIVE_FROM, S.EFFECTIVE_TO)
    """, {"run_id": run_id})

    cursor.execute(f"""
        MERGE INTO POSITIONS T
        USING (
          SELECT PORTFOLIO_ID, SECURITY_ID, POSITION_DATE,
                 QUANTITY, BOOK_VALUE, CURRENCY
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY PORTFOLIO_ID, SECURITY_ID, POSITION_DATE
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM POSITIONS_RAW R WHERE RUN_ID = :run_id
              {_exclude_rejected("positions")}
          ) WHERE RN = 1
        ) S
        ON (T.PORTFOLIO_ID = S.PORTFOLIO_ID
            AND T.SECURITY_ID = S.SECURITY_ID
            AND T.POSITION_DATE = S.POSITION_DATE)
        WHEN MATCHED THEN UPDATE SET
          T.QUANTITY = S.QUANTITY, T.BOOK_VALUE = S.BOOK_VALUE,
          T.CURRENCY = S.CURRENCY
        WHEN NOT MATCHED THEN INSERT
          (PORTFOLIO_ID, SECURITY_ID, POSITION_DATE, QUANTITY, BOOK_VALUE, CURRENCY)
          VALUES
          (S.PORTFOLIO_ID, S.SECURITY_ID, S.POSITION_DATE,
           S.QUANTITY, S.BOOK_VALUE, S.CURRENCY)
    """, {"run_id": run_id})

    cursor.execute(f"""
        MERGE INTO MARKET_PRICES T
        USING (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID, PRICE_DATE, PRICE_SOURCE
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM MARKET_PRICES_RAW R WHERE RUN_ID = :run_id
              {_exclude_rejected("market_prices")}
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID
            AND T.PRICE_DATE = S.PRICE_DATE
            AND T.PRICE_SOURCE = S.PRICE_SOURCE)
        WHEN MATCHED THEN UPDATE SET
          T.PRICE = S.PRICE, T.PRICE_CURRENCY = S.PRICE_CURRENCY
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE)
          VALUES
          (S.SECURITY_ID, S.PRICE_DATE, S.PRICE, S.PRICE_CURRENCY, S.PRICE_SOURCE)
    """, {"run_id": run_id})

    cursor.execute(f"""
        MERGE INTO RESTRICTED_SECURITIES T
        USING (
          SELECT SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM,
                 EFFECTIVE_TO, STATUS
          FROM (
            SELECT R.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM
                     ORDER BY SOURCE_ROW_NUMBER
                   ) RN
            FROM RESTRICTED_SECURITIES_RAW R WHERE RUN_ID = :run_id
              {_exclude_rejected("restricted_securities")}
          ) WHERE RN = 1
        ) S
        ON (T.SECURITY_ID = S.SECURITY_ID
            AND T.RESTRICTION_TYPE = S.RESTRICTION_TYPE
            AND T.EFFECTIVE_FROM = S.EFFECTIVE_FROM)
        WHEN MATCHED THEN UPDATE SET
          T.EFFECTIVE_TO = S.EFFECTIVE_TO, T.STATUS = S.STATUS
        WHEN NOT MATCHED THEN INSERT
          (SECURITY_ID, RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO, STATUS)
          VALUES
          (S.SECURITY_ID, S.RESTRICTION_TYPE, S.EFFECTIVE_FROM,
           S.EFFECTIVE_TO, S.STATUS)
    """, {"run_id": run_id})
    cursor.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT SECURITY_ID, EFFECTIVE_FROM, EFFECTIVE_TO,
                 LEAD(EFFECTIVE_FROM) OVER (PARTITION BY SECURITY_ID ORDER BY EFFECTIVE_FROM) NEXT_FROM
          FROM SECURITY_MASTER
        ) WHERE NEXT_FROM IS NOT NULL
          AND (EFFECTIVE_TO IS NULL OR EFFECTIVE_TO >= NEXT_FROM)
        """
    )
    if cursor.fetchone()[0]:
        raise ValueError("security_master: merged effective-dated history contains overlapping intervals")
    return counts

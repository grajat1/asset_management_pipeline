-- Gold processing SQL. Kept aligned with src/gold_processor.py.
-- Bind :run_id and :business_date. Python remains authoritative for orchestration,
-- transaction validation, reconciliation, delivery registration and publication.
-- Execute in order in one transaction; commit only after every statement succeeds.

DELETE FROM GOLD_COMPLIANCE_EXCEPTIONS WHERE RUN_ID = :run_id;
DELETE FROM GOLD_DATA_QUALITY_EXCEPTIONS WHERE RUN_ID = :run_id;
DELETE FROM GOLD_PORTFOLIO_POSITIONS WHERE RUN_ID = :run_id;

INSERT INTO GOLD_DATA_QUALITY_EXCEPTIONS
          (RUN_ID, BUSINESS_DATE, PORTFOLIO_ID, SECURITY_ID, EXCEPTION_TYPE,
           DETAILS, SOURCE_POSITION_DATE)
        WITH SNAPSHOTS AS (
          SELECT P.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY PORTFOLIO_ID, SECURITY_ID
                   ORDER BY POSITION_DATE DESC
                 ) RN
          FROM POSITIONS P
          WHERE POSITION_DATE <= :business_date
        ),
        BALANCES AS (
          -- Snapshot-backed positions plus post-snapshot transactions.
          SELECT P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                 CASE
                   WHEN COUNT(DISTINCT CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = 0
                     THEN P.CURRENCY
                   WHEN COUNT(DISTINCT CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = 1
                    AND MIN(CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = P.CURRENCY
                     THEN P.CURRENCY
                   ELSE NULL
                 END AS CURRENCY,
                 P.QUANTITY + NVL(SUM(T.QUANTITY), 0) AS CLOSING_QUANTITY
          FROM SNAPSHOTS P
          LEFT JOIN TRANSACTIONS T
            ON T.PORTFOLIO_ID = P.PORTFOLIO_ID
           AND T.SECURITY_ID = P.SECURITY_ID
           AND T.TRADE_DATE > P.POSITION_DATE
           AND T.TRADE_DATE <= :business_date
           AND T.VERSION_STATUS = 'CURRENT'
          WHERE P.RN = 1
          GROUP BY P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                   P.CURRENCY, P.QUANTITY
          UNION ALL
          -- Transaction-only positions must not disappear merely because no
          -- opening snapshot was supplied. Use first trade date as lineage date.
          SELECT T.PORTFOLIO_ID, T.SECURITY_ID, MIN(T.TRADE_DATE),
                 CASE WHEN COUNT(DISTINCT T.CURRENCY) = 1 THEN MIN(T.CURRENCY) ELSE NULL END,
                 SUM(T.QUANTITY)
          FROM TRANSACTIONS T
          WHERE T.TRADE_DATE <= :business_date
            AND T.VERSION_STATUS = 'CURRENT'
            AND NOT EXISTS (
              SELECT 1 FROM POSITIONS P
              WHERE P.PORTFOLIO_ID = T.PORTFOLIO_ID
                AND P.SECURITY_ID = T.SECURITY_ID
                AND P.POSITION_DATE <= :business_date
            )
          GROUP BY T.PORTFOLIO_ID, T.SECURITY_ID
        ),
        LATEST_PRICE AS (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY
          FROM (
            SELECT M.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID
                     ORDER BY PRICE_DATE DESC, CASE UPPER(PRICE_SOURCE) WHEN 'PRIMARY' THEN 1 WHEN 'BLOOMBERG' THEN 2 WHEN 'REUTERS' THEN 3 ELSE 9 END, PRICE_SOURCE
                   ) RN
            FROM MARKET_PRICES M
            WHERE PRICE_DATE <= :business_date
          )
          WHERE RN = 1
        )
        SELECT :run_id, :business_date, B.PORTFOLIO_ID, B.SECURITY_ID,
               CASE
                 WHEN S.SECURITY_ID IS NULL THEN 'UNKNOWN_OR_INACTIVE_SECURITY'
                 WHEN B.CURRENCY IS NULL THEN 'TRANSACTION_CURRENCY_MISMATCH'
                 WHEN UPPER(S.CURRENCY) <> UPPER(B.CURRENCY) THEN 'POSITION_CURRENCY_MISMATCH'
                 WHEN M.SECURITY_ID IS NULL OR M.PRICE IS NULL THEN 'MISSING_MARKET_PRICE'
                 WHEN M.PRICE <= 0 THEN 'INVALID_MARKET_PRICE'
                 WHEN M.PRICE_DATE < :business_date - 3 THEN 'STALE_MARKET_PRICE'
                 WHEN M.PRICE_CURRENCY <> B.CURRENCY THEN 'PRICE_CURRENCY_MISMATCH'
                 ELSE 'VALUATION_WITHHELD'
               END,
               'No eligible security master record or market price; row not published',
               B.POSITION_DATE
        FROM BALANCES B
        LEFT JOIN SECURITY_MASTER S
          ON S.SECURITY_ID = B.SECURITY_ID
         AND :business_date >= S.EFFECTIVE_FROM
         AND (S.EFFECTIVE_TO IS NULL OR :business_date <= S.EFFECTIVE_TO)
        LEFT JOIN LATEST_PRICE M ON M.SECURITY_ID = B.SECURITY_ID
        WHERE S.SECURITY_ID IS NULL
           OR B.CURRENCY IS NULL
           OR UPPER(S.CURRENCY) <> UPPER(B.CURRENCY)
           OR M.SECURITY_ID IS NULL
           OR M.PRICE IS NULL
           OR M.PRICE <= 0
           OR M.PRICE_DATE < :business_date - 3
           OR M.PRICE_CURRENCY <> B.CURRENCY;

INSERT INTO GOLD_PORTFOLIO_POSITIONS (
          RUN_ID, BUSINESS_DATE, PORTFOLIO_ID, SECURITY_ID, QUANTITY,
          MARKET_PRICE, MARKET_VALUE, PRICE_DATE, PRICE_SOURCE, CURRENCY,
          SOURCE_POSITION_DATE
        )
        WITH SNAPSHOTS AS (
          SELECT P.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY PORTFOLIO_ID, SECURITY_ID
                   ORDER BY POSITION_DATE DESC
                 ) RN
          FROM POSITIONS P
          WHERE POSITION_DATE <= :business_date
        ),
        BALANCES AS (
          -- Snapshot-backed positions plus post-snapshot transactions.
          SELECT P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                 CASE
                   WHEN COUNT(DISTINCT CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = 0
                     THEN P.CURRENCY
                   WHEN COUNT(DISTINCT CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = 1
                    AND MIN(CASE WHEN T.TRADE_DATE > P.POSITION_DATE THEN T.CURRENCY END) = P.CURRENCY
                     THEN P.CURRENCY
                   ELSE NULL
                 END AS CURRENCY,
                 P.QUANTITY + NVL(SUM(T.QUANTITY), 0) AS CLOSING_QUANTITY
          FROM SNAPSHOTS P
          LEFT JOIN TRANSACTIONS T
            ON T.PORTFOLIO_ID = P.PORTFOLIO_ID
           AND T.SECURITY_ID = P.SECURITY_ID
           AND T.TRADE_DATE > P.POSITION_DATE
           AND T.TRADE_DATE <= :business_date
           AND T.VERSION_STATUS = 'CURRENT'
          WHERE P.RN = 1
          GROUP BY P.PORTFOLIO_ID, P.SECURITY_ID, P.POSITION_DATE,
                   P.CURRENCY, P.QUANTITY
          UNION ALL
          -- Transaction-only positions must not disappear merely because no
          -- opening snapshot was supplied. Use first trade date as lineage date.
          SELECT T.PORTFOLIO_ID, T.SECURITY_ID, MIN(T.TRADE_DATE),
                 CASE WHEN COUNT(DISTINCT T.CURRENCY) = 1 THEN MIN(T.CURRENCY) ELSE NULL END,
                 SUM(T.QUANTITY)
          FROM TRANSACTIONS T
          WHERE T.TRADE_DATE <= :business_date
            AND T.VERSION_STATUS = 'CURRENT'
            AND NOT EXISTS (
              SELECT 1 FROM POSITIONS P
              WHERE P.PORTFOLIO_ID = T.PORTFOLIO_ID
                AND P.SECURITY_ID = T.SECURITY_ID
                AND P.POSITION_DATE <= :business_date
            )
          GROUP BY T.PORTFOLIO_ID, T.SECURITY_ID
        ),
        LATEST_PRICE AS (
          SELECT SECURITY_ID, PRICE_DATE, PRICE, PRICE_CURRENCY, PRICE_SOURCE
          FROM (
            SELECT M.*,
                   ROW_NUMBER() OVER (
                     PARTITION BY SECURITY_ID
                     ORDER BY PRICE_DATE DESC, CASE UPPER(PRICE_SOURCE) WHEN 'PRIMARY' THEN 1 WHEN 'BLOOMBERG' THEN 2 WHEN 'REUTERS' THEN 3 ELSE 9 END, PRICE_SOURCE
                   ) RN
            FROM MARKET_PRICES M
            WHERE PRICE_DATE <= :business_date
          )
          WHERE RN = 1
        )
        SELECT :run_id, :business_date, B.PORTFOLIO_ID, B.SECURITY_ID,
               B.CLOSING_QUANTITY, M.PRICE,
               ROUND(B.CLOSING_QUANTITY * M.PRICE, 2),
               M.PRICE_DATE, M.PRICE_SOURCE, B.CURRENCY, B.POSITION_DATE
        FROM BALANCES B
        JOIN LATEST_PRICE M ON M.SECURITY_ID = B.SECURITY_ID
        JOIN SECURITY_MASTER S ON S.SECURITY_ID = B.SECURITY_ID
        WHERE M.PRICE IS NOT NULL
          AND M.PRICE > 0
          AND M.PRICE_DATE >= :business_date - 3
          AND M.PRICE_CURRENCY = B.CURRENCY
          AND :business_date >= S.EFFECTIVE_FROM
          AND (S.EFFECTIVE_TO IS NULL OR :business_date <= S.EFFECTIVE_TO);

INSERT INTO GOLD_COMPLIANCE_EXCEPTIONS (
          RUN_ID, TRANSACTION_ID, PORTFOLIO_ID, SECURITY_ID, TRADE_DATE,
          RESTRICTION_TYPE, EXCEPTION_REASON, SOURCE_FILE, SOURCE_ROW_NUMBER
        )
        SELECT :run_id, T.TRANSACTION_ID, T.PORTFOLIO_ID, T.SECURITY_ID,
               T.TRADE_DATE, R.RESTRICTION_TYPE,
               'TRADE DURING ACTIVE RESTRICTION',
               T.SOURCE_FILE, T.SOURCE_ROW_NUMBER
        FROM TRANSACTIONS T
        JOIN RESTRICTED_SECURITIES R
          ON R.SECURITY_ID = T.SECURITY_ID
         AND T.TRADE_DATE >= R.EFFECTIVE_FROM
         AND (R.EFFECTIVE_TO IS NULL OR T.TRADE_DATE <= R.EFFECTIVE_TO)
        WHERE UPPER(R.STATUS) = 'ACTIVE'
          AND T.TRADE_DATE <= :business_date
          AND T.VERSION_STATUS = 'CURRENT';

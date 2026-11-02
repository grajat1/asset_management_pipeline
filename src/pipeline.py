"""Orchestrate the Oracle-backed asset-management data pipeline."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime
from pathlib import Path

from .pipeline_common import (
    DATA_DIR, INPUTS, LOG_LEVEL, LOGGER, connect, delivery_fingerprint,
    file_sha256, insert_run, register_delivery,
)
from .reference_loader import load_reference_and_raw
from .transaction_processor import process_transactions
from .gold_processor import build_gold, write_reconciliation

# Re-export commonly used helpers for backwards compatibility with existing
# imports such as `from src.pipeline import read_csv`.
from .pipeline_common import (  # noqa: F401
    EXPECTED_COLUMNS, OPTIONAL_COLUMNS, ROOT, SCHEMA_VERSION, parse_date,
    parse_decimal, parse_version, raw_insert_sql, raw_values, read_csv,
)

def run_pipeline(
    business_date: date,
    fail_after_load: bool = False,
    fail_after_stage: str | None = None,
    force_replay: bool = False,
    input_paths: dict[str, Path] | None = None,
) -> int:
    input_paths = input_paths or {}
    connection = connect()
    started = time.monotonic()
    cursor = connection.cursor()
    run_id: int | None = None

    try:
        run_id = insert_run(cursor, business_date)
        LOGGER.info("pipeline_started run_id=%s business_date=%s", run_id, business_date)
        connection.commit()  # preserve run record if a later step fails

        # Register all input files before loading. Same set of content hashes
        # means the exact same five-file delivery.
        dataset_hashes = {
            dataset: (
                input_paths.get(dataset, DATA_DIR / default_name).name,
                file_sha256(input_paths.get(dataset, DATA_DIR / default_name)),
            )
            for dataset, (default_name, _) in INPUTS.items()
        }
        combined_hash = delivery_fingerprint(business_date, dataset_hashes)

        should_process = register_delivery(
            cursor, combined_hash, "five-file-delivery", run_id,
            force_replay=force_replay,
        )
        if not should_process:
            cursor.execute(
                """
                UPDATE PIPELINE_RUNS
                SET STATUS = 'SKIPPED_DUPLICATE',
                    COMPLETED_AT = SYSTIMESTAMP,
                    ELAPSED_SECONDS = :elapsed
                WHERE RUN_ID = :run_id
                """,
                {"elapsed": time.monotonic() - started, "run_id": run_id},
            )
            connection.commit()
            LOGGER.info("duplicate_delivery_skipped run_id=%s fingerprint=%s", run_id, combined_hash)
            return run_id
        connection.commit()

        LOGGER.info("bronze_load_started run_id=%s", run_id)
        counts = load_reference_and_raw(cursor, run_id, input_paths)
        total_received = sum(counts.values())
        LOGGER.info("bronze_load_completed run_id=%s dataset_counts=%s", run_id, counts)
        cursor.execute(
            """
            UPDATE PIPELINE_RUNS
            SET RECEIVED_COUNT = :received, STATUS = 'LOADED'
            WHERE RUN_ID = :run_id
            """,
            {"received": total_received, "run_id": run_id},
        )
        connection.commit()

        if fail_after_load or fail_after_stage == "after_load":
            raise RuntimeError("Injected failure after raw load (requested test)")

        received, accepted, rejected, duplicates = process_transactions(
            cursor, run_id, business_date
        )
        LOGGER.info(
            "transaction_validation_completed run_id=%s received=%s accepted=%s rejected=%s duplicates=%s",
            run_id, received, accepted, rejected, duplicates,
        )
        reconciliation_totals = write_reconciliation(
            cursor, run_id, counts, (received, accepted, rejected, duplicates)
        )
        total_accepted, total_rejected, total_duplicates = reconciliation_totals
        if fail_after_stage == "after_transactions":
            raise RuntimeError("Injected failure after transaction validation (requested test)")
        build_gold(cursor, run_id, business_date)
        LOGGER.info("gold_build_completed run_id=%s", run_id)
        if fail_after_stage == "after_gold":
            raise RuntimeError("Injected failure after Gold build and before publication (requested test)")

        # Publish only after all Gold outputs are built. This pointer and the
        # SUCCEEDED status commit atomically, so consumers never see partial runs.
        cursor.execute(
            """
            MERGE INTO GOLD_PUBLISHED_RUNS P
            USING (SELECT :business_date BUSINESS_DATE, :run_id RUN_ID FROM DUAL) S
            ON (P.BUSINESS_DATE = S.BUSINESS_DATE)
            WHEN MATCHED THEN UPDATE SET P.RUN_ID = S.RUN_ID, P.PUBLISHED_AT = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT (BUSINESS_DATE, RUN_ID)
              VALUES (S.BUSINESS_DATE, S.RUN_ID)
            """,
            {"business_date": business_date, "run_id": run_id},
        )
        cursor.execute(
            """
            UPDATE PIPELINE_RUNS
            SET STATUS = 'SUCCEEDED',
                COMPLETED_AT = SYSTIMESTAMP,
                ACCEPTED_COUNT = :accepted,
                REJECTED_COUNT = :rejected,
                DUPLICATE_COUNT = :duplicates,
                ELAPSED_SECONDS = :elapsed
            WHERE RUN_ID = :run_id
            """,
            {
                "accepted": total_accepted,
                "rejected": total_rejected,
                "duplicates": total_duplicates,
                "elapsed": time.monotonic() - started,
                "run_id": run_id,
            },
        )
        cursor.execute(
            "UPDATE FILE_DELIVERIES SET STATUS='SUCCEEDED', COMPLETED_AT=SYSTIMESTAMP WHERE RUN_ID=:run_id",
            {"run_id": run_id},
        )
        cursor.execute(
            "UPDATE FILE_DELIVERY_ATTEMPTS SET ATTEMPT_STATUS='SUCCEEDED', COMPLETED_AT=SYSTIMESTAMP WHERE RUN_ID=:run_id",
            {"run_id": run_id},
        )
        connection.commit()
        LOGGER.info(
            "pipeline_succeeded run_id=%s input_rows=%s transactions_received=%s transaction_accepted=%s transaction_rejected=%s total_accepted=%s total_rejected=%s total_duplicates=%s elapsed_seconds=%.3f",
            run_id, total_received, received, accepted, rejected, total_accepted,
            total_rejected, total_duplicates, time.monotonic() - started,
        )
        return run_id

    except Exception as exc:
        connection.rollback()
        if run_id is not None:
            cursor.execute(
                """
                UPDATE PIPELINE_RUNS
                SET STATUS = 'FAILED', ERROR_MESSAGE = :message,
                    COMPLETED_AT = SYSTIMESTAMP,
                    ELAPSED_SECONDS = :elapsed
                WHERE RUN_ID = :run_id
                """,
                {
                    "message": str(exc)[:1000],
                    "elapsed": time.monotonic() - started,
                    "run_id": run_id,
                },
            )
            cursor.execute("UPDATE FILE_DELIVERIES SET STATUS='FAILED' WHERE RUN_ID=:run_id", {"run_id": run_id})
            cursor.execute("UPDATE FILE_DELIVERY_ATTEMPTS SET ATTEMPT_STATUS='FAILED', COMPLETED_AT=SYSTIMESTAMP WHERE RUN_ID=:run_id", {"run_id": run_id})
            connection.commit()
        LOGGER.exception("pipeline_failed run_id=%s error=%s", run_id, exc)
        raise
    finally:
        cursor.close()
        connection.close()

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--business-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--fail-after-load", action="store_true", help="Backward-compatible failure injection after Bronze load")
    parser.add_argument("--fail-after-stage", choices=("after_load", "after_transactions", "after_gold"))
    parser.add_argument("--force-replay", action="store_true")
    parser.add_argument("--log-level", default=LOG_LEVEL, choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args()
    LOGGER.setLevel(getattr(logging, args.log_level))
    business_date = datetime.strptime(args.business_date, "%Y-%m-%d").date()
    run_pipeline(
        business_date=business_date,
        fail_after_load=args.fail_after_load,
        fail_after_stage=args.fail_after_stage,
        force_replay=args.force_replay,
    )


if __name__ == "__main__":
    main()

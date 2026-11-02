# Change log

## Phase 1 + Phase 2 hardening, plus selected Phase 3 work

- Delivery fingerprint now includes business date, schema version, dataset name, filename and file content hash.
- Security Master supports effective-dated versions; overlapping intervals and conflicting historical updates are rejected.
- Conflicting reference-data duplicates are detected before MERGE; exact duplicates are reconciled.
- Transactions support explicit versioned corrections using `record_version`, `supersedes_version`, and `correction_reason`; prior versions are retained and downstream calculations use only current versions.
- Position calculation includes transaction-only portfolio/security combinations.
- Market-price tie-breaking is deterministic.
- Reconciliation is persisted per dataset.
- Gold publication is exposed through an atomic published-run pointer.
- Failure injection is available after Bronze, transaction validation, and Gold calculation before publication.
- Added unit tests and structured stage/run logging.

## Explicit exclusions

- Concurrent-run protection / database claim locks were intentionally excluded.
- Oracle integration execution and performance/load testing were not possible in this local validation pass and remain required before relying on the SQL changes.

## Python maintainability refactor

- Split the former 1,163-line `src/pipeline.py` into focused modules: orchestration, shared helpers, reference-data loading, transaction processing, and Gold/reconciliation processing.
- Kept the CLI command and commonly imported helper names available through `src.pipeline` for compatibility.
- Removed an unused file-hashing loop that hashed each input once redundantly before the delivery fingerprint was calculated.
- No new class hierarchy was introduced; the existing workflow is function-oriented, so module boundaries provide the separation without unnecessary abstraction.


## Follow-up code review corrections

- Fixed missing `datetime` and `hashlib` imports and added a direct transaction-processing test using a datetime trade date.
- Canonicalized transaction hashes with sorted JSON and scale-independent decimal values; non-finite decimal inputs (`NaN`, `Infinity`) are rejected.
- Enforced consecutive correction versions and explicit supersedes links; added handling for inconsistent historical/current version state.
- Replaced per-transaction Security Master lookups with a cached effective-dated reference map and batched existing-transaction history lookups.
- Added row-level `REFERENCE_DATA_REJECTS` for invalid reference records and excluded rejected rows from curated merges.
- Added effective Security Master reference checks for positions, prices and restrictions; added position currency validation. Controlled Security Master versioning now permits closing an open interval only when its next effective version starts the following day.
- Added overlapping restricted-security interval validation and expanded Security Master overlap validation across existing and incoming history.
- Corrected Gold exception classification for null prices, inactive/unknown Security Master records and currency inconsistencies; transaction-only balances with mixed currencies are withheld.
- Added `FILE_DELIVERY_ATTEMPTS` so skipped deliveries and forced replays retain attempt history.
- Bumped the schema fingerprint version to `1.2` so the updated schema/code contract has a distinct delivery identity.
- Reconciliation now accounts for all five reference datasets plus transactions, and pipeline run totals reflect all datasets.
- Aligned `sql/03_process_run.sql` with the Gold SQL executed by Python.
- Expanded tests from 11 to 23.

## Validation limitation

- Python unit tests, compilation and CLI checks pass. Oracle DDL/DML execution and full database-backed replay/publication tests remain unverified because no Oracle instance was available in this environment.

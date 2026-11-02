# Engineering Decision Record

## Context

The case study requires a reliable, auditable pipeline for daily asset-management inputs and trusted portfolio/compliance outputs. Synthetic data only is used. This revision implements the agreed Phase 1 and Phase 2 scope, excluding concurrent-run protection, plus unit tests and improved logging from Phase 3.

## Decisions and implemented controls

### 1. Layering and lineage
Use Bronze raw tables, Silver accepted/rejected records, and Gold business outputs. Raw inputs retain run ID, source filename and source row number.

### 2. Delivery idempotency
- Delivery fingerprint includes business date, schema version, dataset identity, filename and SHA-256 content hash for each input.
- Identical successful deliveries for the same business date are skipped.
- Different business dates or changed dataset content produce different delivery identities.

### 3. Transaction idempotency
- Transaction business key remains `TRANSACTION_ID`.
- Identical transaction payloads are counted and ignored.
- Conflicting payloads with the same transaction ID are quarantined; no silent overwrite occurs.
- Optional input columns `record_version`, `supersedes_version`, and `correction_reason` support explicit corrections. Version defaults to 1. A correction must be the next consecutive version, supersede the current version, and include a reason. Old versions remain in Silver with `SUPERSEDED`; downstream position/compliance queries use only `CURRENT`. Same-version conflicts, stale versions, version gaps and invalid supersedes references are quarantined. Decimal values are canonicalized before hashing so formatting differences such as `10.0` and `10.00` do not create false conflicts. The source must guarantee authoritative monotonic versioning; business must approve reversal/restatement semantics.

### 4. Reference-data duplicates and temporal logic
- Exact duplicate reference rows are collapsed and included in duplicate reconciliation counts.
- Conflicting values for the same natural key fail the delivery rather than allowing arbitrary `ROW_NUMBER()` selection. Row-level reference defects (missing required fields, invalid intervals, unknown/inactive securities, and position/security currency mismatches) are recorded in `REFERENCE_DATA_REJECTS` and excluded from Silver merges.
- Security Master natural key is `(SECURITY_ID, EFFECTIVE_FROM)`; multiple effective-dated versions are retained. A previously open-ended interval may be closed only when the incoming delivery also supplies the next version starting the following day; other changes to an existing natural key are rejected. Effective-date validation uses the incoming-over-existing history view, so the old open-ended row is not incorrectly treated as still active after its controlled closure. Overlapping intervals are checked across existing history and the incoming delivery. Restricted-security intervals are also checked for overlap per `(SECURITY_ID, RESTRICTION_TYPE)` before merge.
- Position snapshot is the latest snapshot on or before the business date. Transactions after the selected snapshot date and on or before the business date are applied. Transaction-only position combinations are included.
- Price is latest on or before business date, maximum age three calendar days. Ties use illustrative precedence PRIMARY, BLOOMBERG, REUTERS, then other sources; business approval is required.
- Restriction interval is inclusive effective-from/effective-to dates and status ACTIVE.

### 5. Missing or stale prices
Withhold valuation rows with missing, stale, non-positive or currency-mismatched prices. Write a data-quality exception. A production publication policy should decide whether any exception blocks all output or only affected portfolios.

### 6. Reconciliation
`PIPELINE_RECONCILIATION` stores per-dataset received, accepted, rejected and duplicate counts. Categories are mutually exclusive for transaction records. Reference-data duplicate counts are based on repeated natural keys after conflicting keys have been rejected.

### 7. Atomic publication and recovery
- Gold outputs are run-scoped.
- Consumers resolve the published run through `GOLD_PUBLISHED_RUNS`; they must not consume arbitrary in-progress run IDs.
- The published-run pointer and `SUCCEEDED` status are committed in the same transaction after Gold and reconciliation complete.
- Failure injection is supported after Bronze load, after transaction validation, and after Gold calculation but before publication.
- Injected failures after the Bronze commit retain run metadata and Bronze rows; transaction/Gold work is rolled back before publication. A new run can safely replay the same delivery. A validation failure during the Bronze/reference load transaction rolls back that load, so the input file and failed run metadata remain the external audit evidence for that failed attempt.

### 8. Security and logging
Credentials come from environment variables, not source code. Logs include timestamp, level, run ID, business date, processing stage and aggregate counts. Do not log credentials or sensitive source payloads. Use least-privilege DB grants, secret management, TLS/wallet configuration, controlled log access and retention. Synthetic data only.

## Intentionally excluded from this revision

- Concurrent-run protection, database claim locks and overlapping execution coordination. Do not run simultaneous executions for the same delivery/business date.
- Business sign-off on transaction correction/reversal semantics and downstream historical restatement policy. Conflicts remain quarantined.
- Full Oracle integration tests, load tests, FX conversion, corporate actions, settlement/lot accounting, accrued interest, regulatory sign-off, dashboards and distributed orchestration.

## Scaling considerations

The prototype targets a small assessment dataset. For higher volume, stream CSVs and use bounded `executemany()` batches, index join/date columns based on execution plans, partition large transaction history where justified, and benchmark realistic distributions before selecting parallel/distributed processing. The current transaction processor prefetches Security Master history and existing transaction versions in bounded batches, but accepted transaction inserts and correction supersedes updates are still row-oriented and should be batched after profiling if volumes justify it.

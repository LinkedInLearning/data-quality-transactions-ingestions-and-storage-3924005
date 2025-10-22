# Data Platform Product Requirements Document (PRD)

## 1. Data Platform Architecture Overview

### Current Architecture

![](course_assets/project_walkthrough_images/00_02_data_platform_architecture.png)

The platform centers on a PostgreSQL transactional database for ingestion and storage of operational datasets, a MinIO object store acting as the data lake, and DuckDB as a lightweight lakehouse/analytics engine. Ingestion loads CSVs into PostgreSQL with schema-driven table creation, primary keys, and idempotent upserts. Replication exports tables from PostgreSQL to MinIO as Avro files, partitioned by date-like path components, and DuckDB reads the latest Avro artifact into a `staging` schema for analysis.

A Write–Audit–Publish (WAP) flow is available: rows first land in `staging_*` tables, failing rows are redirected to `quarantine_*` tables, and only audited, conforming data is published downstream. End-to-end execution is orchestrated via simple runner scripts.

**Key Components:**
- **Ingestion (CSV → PostgreSQL)**: `data_platform/scripts/run_ingestion.py` → `data_platform/ingestion/postgres_ingestion_csv.py`
- **Clients**: `data_platform/clients/postgres_client.py`, `data_platform/clients/minio_client.py`, `data_platform/clients/duckdb_client.py`
- **Configs**: `data_platform/config/postgres_config.py`, `data_platform/config/minio_config.py`, `data_platform/config/duckdb_config.py`
- **Replication (PostgreSQL → MinIO Avro)**: `data_platform/etl/batch_postgres_minio_etl.py`, `data_platform/scripts/run_etl.py`
- **Lakehouse ELT (MinIO Avro → DuckDB)**: `data_platform/etl/batch_minio_duckdb_elt.py`
- **WAP ETL**: `data_platform/etl/batch_postgres_minio_wap_etl.py`, `data_platform/scripts/run_wap_etl.py`
- **Transactions Simulator**: `data_platform/transactions/concurrency_simulator.py`
- **Data, Schemas, and Samples**: `data_platform/data/**`

## 2. Gap Analysis

### 2.1 Ingestion into Transactional Database

Here's the current state in markdown format:

**Current State:**

- **Schema validation**: Uses JSON schema files to define table structure with specific data types
- **Data cleaning**: Automatically removes empty rows and standardizes column names
- **Duplicate prevention**: Implements UPSERT operations using `ON CONFLICT` to handle duplicate primary keys
- **Audit trails**: Adds automatic `pg_created_at` and `pg_updated_at` timestamps with update triggers
- **Idempotent processing**: Can run the same ingestion multiple times without creating duplicates
- **Type safety**: Enforces PostgreSQL data types defined in schema (BIGINT, TEXT, DATE, INTEGER, etc.)
- **Primary key constraints**: Ensures data uniqueness through primary key definitions
- **Comprehensive logging**: Detailed logging at DEBUG, INFO, and ERROR levels for troubleshooting
- **Transaction safety**: Uses atomic database transactions to ensure all data operations complete entirely or not at all, preventing partial data corruption
- **Error handling**: Validates file existence and includes proper exception handling

**Gaps Found:**
- **Business rule constraints missing**: Negative or invalid values can pass schema typing. Example observed in the notebook: `parking_violation_codes.code = -99` with definition `TEST` ingested successfully, then surfaced in reports.
- **No referential integrity between facts and dimensions**: `parking_violations_issued.violation_code` is not enforced to exist in `parking_violation_codes.code`, permitting orphan codes and inconsistent joins.
- **Over-permissive text fields**: Fields like `vehicle_expiration_date` are `TEXT` and contain placeholders like `0` or `88888888`, which are semantically invalid dates and propagate downstream.
- **Nullability not curated for downstream**: Many columns permit NULLs; later stages (Avro writing) may expect integers and fail when NULLs appear for integer fields.
- **Ingestion lacks quarantine at the source**: Validation is primarily type/shape oriented; domain violations are only caught later in WAP or analytics, not at the initial landing.

**Recommendations:**
- **Add database CHECK constraints** to enforce domain rules at write time. Proven examples from the notebook:
  - `ALTER TABLE parking_violation_codes ADD CONSTRAINT code_check CHECK (code >= 0);`
  - `ALTER TABLE parking_violations_issued ADD CONSTRAINT summons_number_is_ten_digits CHECK (summons_number::text ~ '^[0-9]{10}$');`
- **Add foreign key constraints** to maintain referential integrity:
  - `ALTER TABLE parking_violations_issued ADD CONSTRAINT fk_violation_code FOREIGN KEY (violation_code) REFERENCES parking_violation_codes(code);`
- **Tighten data types and casting**:
  - Store true dates in `DATE` columns (e.g., normalize `vehicle_expiration_date` or persist as NULL when invalid).
  - Ensure numeric fine columns are `INTEGER` and default to NULL if missing, not strings.
- **Introduce ingestion-time quarantine**:
  - Capture rows that fail constraints into `quarantine_*` tables with error reasons, not just fail the batch.
- **Extend JSON schemas with rule hints** (ranges, regex) and surface them in ingestion validations, not only in DDL.
- **Document allowed enumerations** (e.g., `registration_state`, `plate_type`) and validate against them during ingestion.

### 2.2 Transactions on Transactional Database
**Current State:** Concurrent writers can operate at PostgreSQL’s default `READ COMMITTED` isolation. The `TransactionSimulator` demonstrates nondeterministic totals under contention when two workers update the same logical record without coordination.

In the notebook, repeated runs of `simulator.simulate(increment_a=1, increment_b=2, iterations=5)` produced different results at default isolation. When the isolation was set to `SERIALIZABLE`, results stabilized at the expected total (15), with transient retries on serialization failures.

**Gaps Found:**
- **Potential for lost updates/serialization anomalies** under default isolation and concurrent writers.
- **No standardized retry policy** for serialization failures across write paths.
- **No explicit locking pattern** for critical read-modify-write sections.

**Recommendations:**
- **Use `SERIALIZABLE` isolation** for high-risk transactional sections or adopt `SELECT ... FOR UPDATE` to serialize access to hot rows.
- **Implement bounded retries** on `could not serialize access due to concurrent update` with jitter backoff (the simulator retried up to 4x successfully).
- **Add metrics/logging** for serialization failures and retries to tune contention handling.

### 2.3 Replication to Data Lakehouse
**Current State:** `batch_postgres_minio_etl.py` exports each public PostgreSQL table to Avro and writes to MinIO under date-partitioned keys (e.g., `postgres-exports/<table>/YYYY/MM/DD/<table>_timestamp.avro`). `batch_minio_duckdb_elt.py` then reads the latest object per table into DuckDB `staging.<table>`. The WAP pipeline (`batch_postgres_minio_wap_etl.py`) writes to `staging_*` and `quarantine_*` tables in PostgreSQL before exporting curated Avro snapshots.

**Gaps Found:**
- **Type nullability mismatch caused ETL failure**: Avro writer raised `TypeError: an integer is required on field manhattan_96th_st_below` when NULLs were present for an `int` field.
- **Schema evolution and contracts are implicit**: Schemas are inferred from JSON table schemas; Avro unions for nullability aren’t consistently declared.
- **Publishing policy not uniform**: The basic ETL writes all tables; only the WAP flow enforces per-row audits/quarantine before publish.

**Recommendations:**
- **Define explicit Avro schemas with unions for optional fields**, e.g., `["null", "long"]` for integer columns that can be missing.
- **Coerce and validate types in transforms**: Convert placeholder strings like `""`, `"0"`, `"88888888"` to NULLs before serialization.
- **Standardize on WAP for production**: Always stage and audit; only publish if checks pass, while retaining quarantined records with reasons.
- **Retention and snapshotting**: Keep multiple dated exports (already present) and implement lifecycle policies to manage storage.

## 3. Improvement Suggestions

### Priority 1 (High Impact)
**Improvement:** Enforce domain integrity and referential integrity in PostgreSQL
- **What**: Add CHECKs (e.g., `code >= 0`, 10-digit `summons_number`) and FKs (`violations_issued.violation_code` → `violation_codes.code`).
- **Why**: Prevent bad data from entering the system; ensures consistent joins and analytics.
- **Tradeoffs**: Stricter writes may initially reject legacy rows; requires migration/backfill to conform.
- **Test Results**: After adding `code_check`, attempts to ingest negative codes failed with:

```bash
CheckViolation: new row for relation "parking_violation_codes" violates check constraint "code_check"
DETAIL:  Failing row contains (-99, TEST, null, null, ...).
```

### Priority 1 (High Impact)
**Improvement:** Adopt Write–Audit–Publish (WAP) as the default replication pattern
- **What**: Land rows in `staging_*`, route invalid rows to `quarantine_*`, export only staged/passed data to MinIO and DuckDB.
- **Why**: Prevents broken snapshots and propagating bad records; keeps explainability via quarantined examples.
- **Tradeoffs**: More tables and control flow to manage; slightly more compute/storage.
- **Test Results**: With WAP, valid tables exported and loaded; invalid rows were visible in `quarantine_*` and did not break downstream ELT.

### Priority 2 (Medium Impact)
**Improvement:** Make Avro schemas explicitly nullable for optional numeric fields
- **What**: Change Avro types from `long` → `["null","long"]` and ensure transforms map missing/invalid values to NULL.
- **Why**: Prevent writer failures like `TypeError: an integer is required on field manhattan_96th_st_below`.
- **Tradeoffs**: Consumers must handle NULLs; contract needs documentation.
- **Test Results**: Expected to resolve the observed `fastavro` error seen during `run_etl.py`.

### Priority 2 (Medium Impact)
**Improvement:** Strengthen transaction semantics for hot updates
- **What**: Use `SERIALIZABLE` isolation or row-level locks with bounded retries for conflicting writers.
- **Why**: Eliminates lost updates and nondeterministic outcomes.
- **Tradeoffs**: Higher contention may increase retries/latency; needs careful scoping.
- **Test Results**: Simulator produced stable, expected totals (15) with `SERIALIZABLE` and retries.

### Priority 3 (Low Impact)
**Improvement:** Enumerations and data dictionaries for key fields
- **What**: Validate `registration_state`, `plate_type`, `issuing_agency` against known sets or lookup tables.
- **Why**: Reduces downstream surprises and improves data quality SLAs.
- **Tradeoffs**: Requires curation and periodic updates.
- **Test Results**: N/A (design enhancement).

## 4. Remaining Questions

- **How should incremental ingestion and CDC be handled?** Current jobs are batch-oriented; deletes/updates are not explicitly modeled.
- **What is the authoritative contract for Avro schemas?** Should we version and publish them, or continue to infer from table schemas?
- **Who owns quarantine triage and remediation SLAs?** Define ownership, turnaround, and auto-retry policies.
- **Are there PII/governance requirements?** If so, classify fields and apply masking/access controls.
- **What are SLOs for freshness and failure budgets?** Define alerting and error budgets for ingestion and replication.

## 5. Next Steps

**Immediate Actions:**
- Add PostgreSQL constraints: `code_check`, `summons_number_is_ten_digits`, and `fk_violation_code`.
- Update Avro schema generation in `batch_postgres_minio_etl.py` to emit nullable types for optional integers and coerce invalid strings to NULL.
- Default to WAP: run `data_platform/scripts/run_wap_etl.py` in place of the basic ETL for routine replication.
- Document enumerations and add validations in `postgres_ingestion_csv.py`.

**Future Considerations:**
- Introduce CDC/incremental loads; consider Debezium or logical replication for near-real-time export.
- Automate schema/version management and contract testing for Avro.
- Add data quality dashboards and alerts (row rejection rates, serialization retries, latency).
- Evaluate partitioning, lifecycle, and cost management policies in MinIO buckets.

### File References (for reviewers)
- `data_platform/ingestion/postgres_ingestion_csv.py`
- `data_platform/clients/postgres_client.py`
- `data_platform/config/postgres_config.py`
- `data_platform/scripts/run_ingestion.py`
- `data_platform/etl/batch_postgres_minio_etl.py`
- `data_platform/etl/batch_minio_duckdb_elt.py`
- `data_platform/etl/batch_postgres_minio_wap_etl.py`
- `data_platform/scripts/run_etl.py`
- `data_platform/scripts/run_wap_etl.py`
- `data_platform/transactions/concurrency_simulator.py`
- `data_platform/clients/minio_client.py`
- `data_platform/clients/duckdb_client.py`
- `data_platform/data/schemas/*.json`
---
name: dbt_generate_staging_with_dedupe_and_code_normalization
description: Generate stable staging models (stg_*) from bootstrapped sources. Dedupe + normalize codes using project macros. Minimal casting — pass through native types wherever possible.
tools: []
---

## Goal
For each table defined in the source YAML created by skill `01_bootstrap_sources`, create a staging model that:
1. Selects from the corresponding `source('{source_name}', '{table}')` 
2. Renames columns to `snake_case` (if not already)
3. Deduplicates by natural key using the `dedupe_latest` macro
4. Normalizes code/status fields using project macros
5. Passes through all other columns with **zero type casting** unless explicitly required

## Required Inputs (ask if missing)
| Parameter       | Description                                    | Example         |
|-----------------|------------------------------------------------|-----------------|
| `source_name`   | dbt source name from step 01                   | `raw_acq`       |
| `source_path`   | Location of `_sources.yml` from step 01        | `models/staging/raw_acq/` |

Before generating any SQL, **read the `_sources.yml` file** from step 01 to confirm:
- Which tables exist
- Which columns exist per table
- Column data types (from the INFORMATION_SCHEMA discovery)

Do NOT assume columns exist — only reference columns listed in `_sources.yml`.

## Hard Rules (do not violate)

### Compilation Safety
- Every staging model MUST compile and run even if raw data is dirty.
- Never reference a column not defined in `_sources.yml`.
- Never `ref()` a model that doesn't exist yet.
- Never use `*` (SELECT star) — always explicit column lists.

### Type Casting Rules (CRITICAL — most common cause of build failures)
- **DEFAULT: No casting.** Pass columns through as-is from the source.
- **Timestamps** (`created_at`, `updated_at`, `*_date`): Pass through as-is. Do NOT use `try_to_timestamp`, `to_timestamp`, `cast(... as timestamp)`, or any timestamp conversion function. Snowflake loads these correctly.
- **String columns**: Pass through as-is. Do NOT cast to `VARCHAR` or `STRING`.
- **Numeric columns** (`*_amount`, `*_premium`, `*_cost`): Use `{{ safe_to_number(column_name, scale=2) }}` ONLY IF the column is a `VARCHAR`/`STRING` type in `_sources.yml`. If it's already `NUMBER`/`FLOAT`/`DECIMAL`, pass through as-is.
- **ID columns**: Pass through as-is. Do NOT cast IDs — they may be numeric or string depending on the source system.
- **When in doubt: do not cast.** A pass-through that works beats a cast that breaks.

### Macro Usage
Use ONLY these project macros. Do not write inline equivalents:
| Macro | Purpose | When to Use |
|-------|---------|-------------|
| `{{ dedupe_latest(relation, key_cols, updated_at_col) }}` | Deduplicate by PK using latest `updated_at` | Every staging model — wrap the CTE |
| `{{ normalize_policy_status(col) }}` | Standardize policy status codes | `stg_raw_policy.policy_status` |
| `{{ normalize_lob(col) }}` | Standardize line of business codes | `stg_raw_policy.line_of_business` |
| `{{ normalize_claim_status(col) }}` | Standardize claim status codes | `stg_raw_claim.claim_status` |
| `{{ normalize_txn_type(col) }}` | Standardize transaction type codes | `stg_raw_claim_txn.txn_type` |
| `{{ safe_to_number(col, scale=2) }}` | Safe string-to-number conversion | ONLY on VARCHAR amount columns |

- Do NOT write inline `CASE WHEN` statements for normalization. Use the macros.
- Do NOT create new macros. Use only what exists.

### File Placement
- SQL models go in `models/staging/{source_name}/`
- Schema YAML goes in `models/staging/{source_name}/_staging_tests.yml`
- Naming: `stg_{source_name}__{table_name_without_raw_prefix}.sql`
  - Example: source table `raw_customer` → `stg_raw_acq__customer.sql`

## Model Pattern (follow exactly)

```sql
-- models/staging/{source_name}/stg_{source_name}__{entity}.sql

with source as (
    select * from {{ source('{source_name}', '{table_name}') }}
),

deduped as (
    {{ dedupe_latest('source', ['{pk_column}'], 'updated_at') }}
),

renamed as (
    select
        -- keys
        {pk_column},

        -- dimensions (pass through, normalize codes only)
        {dimension_columns},

        -- amounts (safe_to_number ONLY if source type is VARCHAR)
        {amount_columns},

        -- timestamps (pass through as-is, never cast)
        {timestamp_columns}

    from deduped
)

select * from renamed
```

### Important Pattern Notes
- The `source` CTE selects `*` from the source — this is the ONLY place `*` is allowed.
- The `renamed` CTE must list columns explicitly — this is where renaming and normalization happen.
- Amounts: use `safe_to_number()` ONLY if the source column type is VARCHAR. Otherwise just reference the column directly.
- Timestamps: just list them. No functions. `updated_at` not `try_to_timestamp(updated_at)`.

## Staging Models to Generate

### stg_{source_name}__customer
| Output Column   | Source Column   | Treatment                |
|-----------------|-----------------|--------------------------|
| `customer_id`   | `customer_id`   | Pass through             |
| `customer_name` | `customer_name` | Pass through (nulls OK)  |
| `customer_type` | `customer_type` | Pass through             |
| `state`         | `state`         | Pass through             |
| `created_at`    | `created_at`    | Pass through (no cast)   |
| `updated_at`    | `updated_at`    | Pass through (no cast)   |

- Dedupe key: `customer_id`

### stg_{source_name}__policy
| Output Column      | Source Column      | Treatment                        |
|--------------------|--------------------|----------------------------------|
| `policy_id`        | `policy_id`        | Pass through                     |
| `policy_number`    | `policy_number`    | Pass through                     |
| `customer_id`      | `customer_id`      | Pass through                     |
| `line_of_business` | `line_of_business` | `{{ normalize_lob('line_of_business') }}` |
| `carrier`          | `carrier`          | Pass through                     |
| `effective_date`   | `effective_date`   | Pass through (no cast)           |
| `expiration_date`  | `expiration_date`  | Pass through (no cast)           |
| `written_premium`  | `written_premium`  | `safe_to_number` only if VARCHAR |
| `policy_status`    | `policy_status`    | `{{ normalize_policy_status('policy_status') }}` |
| `updated_at`       | `updated_at`       | Pass through (no cast)           |

- Dedupe key: `policy_id`

### stg_{source_name}__claim
| Output Column      | Source Column      | Treatment                        |
|--------------------|--------------------|----------------------------------|
| `claim_id`         | `claim_id`         | Pass through                     |
| `policy_id`        | `policy_id`        | Pass through                     |
| `loss_date`        | `loss_date`        | Pass through (no cast)           |
| `reported_date`    | `reported_date`    | Pass through (no cast)           |
| `claim_status`     | `claim_status`     | `{{ normalize_claim_status('claim_status') }}` |
| `incurred_amount`  | `incurred_amount`  | `safe_to_number` only if VARCHAR |
| `paid_amount`      | `paid_amount`      | `safe_to_number` only if VARCHAR |
| `updated_at`       | `updated_at`       | Pass through (no cast)           |

- Dedupe key: `claim_id`

### stg_{source_name}__claim_txn
| Output Column      | Source Column      | Treatment                        |
|--------------------|--------------------|----------------------------------|
| `claim_txn_id`     | `claim_txn_id`     | Pass through                     |
| `claim_id`         | `claim_id`         | Pass through                     |
| `txn_type`         | `txn_type`         | `{{ normalize_txn_type('txn_type') }}` |
| `txn_date`         | `txn_date`         | Pass through (no cast)           |
| `txn_amount`       | `txn_amount`       | `safe_to_number` only if VARCHAR |
| `updated_at`       | `updated_at`       | Pass through (no cast)           |

- Dedupe key: `claim_txn_id`

## Schema Tests (`_staging_tests.yml`)

Keep tests minimal and demo-safe:

```yaml
version: 2

models:
  - name: stg_{source_name}__customer
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique

  - name: stg_{source_name}__policy
    columns:
      - name: policy_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null:
              severity: warn
          - relationships:
              to: ref('stg_{source_name}__customer')
              field: customer_id
              severity: warn

  - name: stg_{source_name}__claim
    columns:
      - name: claim_id
        tests:
          - not_null
          - unique
      - name: policy_id
        tests:
          - not_null:
              severity: warn
          - relationships:
              to: ref('stg_{source_name}__policy')
              field: policy_id
              severity: warn

  - name: stg_{source_name}__claim_txn
    columns:
      - name: claim_txn_id
        tests:
          - not_null
          - unique
      - name: claim_id
        tests:
          - not_null:
              severity: warn
          - relationships:
              to: ref('stg_{source_name}__claim')
              field: claim_id
              severity: warn
```

### Test Rules
- `unique` + `not_null` on every PK column → `severity: error` (these must pass post-dedupe)
- `not_null` on FK columns → `severity: warn`
- `relationships` on FK columns → `severity: warn`
- Do NOT add `accepted_values` tests — normalization macros handle this and values vary by acquisition
- Do NOT add rowcount, recency, or expression tests

## Error Recovery
If `dbt build -s staging` fails:
1. Read the error message carefully
2. If **compilation error**: check that every column referenced exists in `_sources.yml`
3. If **database error**: a column name is wrong or a macro doesn't exist — verify macro names match exactly
4. If **test failure at error severity**: the dedupe didn't work — check the `dedupe_latest` key column
5. If **test failure at warn severity**: this is expected for acquisitions — proceed
6. Apply the smallest possible fix, then re-run `dbt build -s staging`
7. If stuck, use skill `05_fix_failures_fast`

## When Done
1. Confirm all staging models compile: `dbt parse`
2. Run: `dbt build -s staging`
3. Report results — passes, warns (expected), and any errors
4. Recommend proceeding to skill `03_build_facts_incremental`

## What NOT to Do
- Do NOT modify `_sources.yml` or `_sources_tests.yml` from step 01
- Do NOT modify `dbt_project.yml`
- Do NOT modify or create macros
- Do NOT cast timestamps
- Do NOT cast IDs
- Do NOT cast amounts that are already numeric
- Do NOT write inline CASE statements — use normalization macros
- Do NOT add accepted_values tests
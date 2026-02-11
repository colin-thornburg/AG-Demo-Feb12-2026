---
name: dbt_generate_staging_with_dedupe_and_code_normalization
description: Generate stable staging models (stg_*) with dedupe + code normalization using project macros.
tools: []
---

## Goal
Create `stg_raw_*` models that:
- select from `source('raw_acq', ...)`
- rename columns to snake_case
- normalize code values via macros (do NOT inline long CASE statements)
- dedupe by natural key using latest `updated_at`

## Hard rules (demo safety)
- Every staging model MUST compile even if raw data is dirty.
- Staging is normalization + dedupe only; do not perform type casting in staging
- Only use safe_to_number() if you explicitly encounter a string amount column
- Use the macros:
  - `{{ dedupe_latest(relation, key_cols, updated_at_col) }}`
  - `{{ normalize_policy_status(col) }}`, `{{ normalize_lob(col) }}`, `{{ normalize_claim_status(col) }}`, `{{ normalize_txn_type(col) }}`
  - `{{ safe_to_number(col, scale=2) }}` when casting amounts
- Never reference columns not in source.
- No refs to non-existent models.
- Do not cast timestamps with try_to_timestamp_*; pass through created_at/updated_at as-is.

## Required staging models
- `models/staging/stg_raw_customer.sql`
- `models/staging/stg_raw_policy.sql`
- `models/staging/stg_raw_claim.sql`
- `models/staging/stg_raw_claim_txn.sql`

## Column expectations (keep minimal, stable)
### stg_raw_customer
- customer_id (string)
- customer_name (string, allow null)
- customer_type (string)
- state (string)
- created_at (timestamp)
- updated_at (timestamp)

### stg_raw_policy
- policy_id, policy_number, customer_id
- line_of_business (normalized)
- carrier
- effective_date, expiration_date
- written_premium (number)
- policy_status (normalized)
- updated_at

### stg_raw_claim
- claim_id, policy_id
- loss_date, reported_date
- claim_status (normalized)
- incurred_amount, paid_amount (number)
- updated_at

### stg_raw_claim_txn
- claim_txn_id, claim_id
- txn_type (normalized)
- txn_date
- txn_amount (number)
- updated_at

## Add schema tests for staging (safe)
Create `models/staging/schema.yml`:
- `unique` + `not_null` on *_id columns in staging (post-dedupe).
- accepted values on normalized fields (pc/benefits/cyber etc).
- relationships staging->staging with `severity: warn` (messy acquisitions).

## When done
Execute commands:
- `dbt build -s staging`

## if there is an error in the logs or the build is not successful because of a syntax error, fix the error and re-excute
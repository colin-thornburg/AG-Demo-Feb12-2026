---
name: dbt_bootstrap_sources_snowflake
description: Create dbt sources + schema tests for Snowflake raw acquisition tables (AG_DEMO.RAW_ACQ.*).
tools: []
---

## Goal
Given a Snowflake raw schema, create:
1) `models/sources.yml` with properly defined sources/tables/columns
2) `models/sources_schema.yml` tests for basic integrity
3) A short README note of assumptions

## Required inputs (ask if missing)
- database: default `AG_DEMO`
- schema: default `RAW_ACQ`
- tables: default `RAW_CUSTOMER`, `RAW_POLICY`, `RAW_CLAIM`, `RAW_CLAIM_TXN`

## Hard rules (do not violate)
- Use snake_case for all dbt model/column names.
- Do NOT write custom SQL for business logic here.
- Add tests only where they won't be flaky (avoid strict rowcount checks).

## Output files
- `models/sources.yml`
- `models/sources_schema.yml`

## Source definitions
- Source name: `raw_acq`
- Database: `AG_DEMO`
- Schema: `RAW_ACQ`
- Put all 4 tables under the same source.

## Tests to include (safe defaults)
- `RAW_CUSTOMER.customer_id`: `not_null`, `unique`
- `RAW_POLICY.policy_id`: `not_null`, `unique` (duplicates exist in raw but will be handled in staging; still keep `not_null`)
- `RAW_CLAIM.claim_id`: `not_null`, `unique` (unique in raw generator)
- `RAW_CLAIM_TXN.claim_txn_id`: `not_null`, `unique`

Relationships (only if reliable)
- `RAW_POLICY.customer_id` -> `RAW_CUSTOMER.customer_id` as a `relationships` test BUT set `severity: warn` (acquisitions are messy).
- `RAW_CLAIM.policy_id` -> `RAW_POLICY.policy_id` also `severity: warn`.

## Documentation
Add a doc string on the source describing "acquired entity raw export" and call out known issues:
- inconsistent code values
- duplicates
- null names
- late updates

## When done
Provide a short checklist:
- `dbt parse`
- `dbt build -s source:raw_acq`

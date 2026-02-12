---
name: dbt_generate_staging_dynamic
description: Generate stable staging models for any source using dedupe + normalization patterns, and iterate using dbt CLI feedback.
tools: []
---

## What this skill does (high-level)
Given a dbt source (and minimal table metadata), generate staging models that:
- select from `source(<source_name>, <table_name>)`
- standardize naming (snake_case)
- dedupe by natural key using latest updated-at column
- normalize known enums using macros when provided
- add safe tests (unique/not_null enums + warn relationships)

This skill is meant to be **portable** across domains:
- insurance claims/policies
- ecommerce orders/payments
- SaaS subscriptions/events
- healthcare encounters/claims
etc.

## Required inputs (must be provided by the caller)
Provide a small “table map” in the prompt. Example format:

tables:
  - staging_model: stg_<domain>__<entity>   # e.g. stg_sales__orders
    source_name: <source_name>              # e.g. raw_acq
    source_table: <table_in_sources_yml>    # e.g. raw_policy
    identifier_column_types: optional       # only if raw is VARIANT/STRING-heavy
    primary_key: [<col1>, <col2>]           # e.g. [policy_id]
    updated_at: <updated_at_col>            # e.g. updated_at
    select_columns:
      - <colA>
      - <colB>
    enum_normalizations:                    # optional
      <col>: <macro_name>                   # e.g. policy_status: normalize_policy_status
    relationships:                          # optional; will be warn severity
      - column: <fk_col>
        to_model: <staging_model_name>
        field: <pk_col>

### Notes / examples
- If a new source table arrives, you add one new table entry and rerun this skill.
- If you don't know the PK/updated_at yet, run `dbt ls --resource-type source -s source:<source_name>.*` and inspect the raw schema in Snowflake.

## Hard rules (demo & production safety)
- Do NOT modify dbt_project.yml, profiles.yml, or existing source YAML unless explicitly asked.
- Do NOT create new macros unless explicitly asked. Prefer existing macros in `macros/`.
- Avoid TRY_CAST/TRY_TO_* on Snowflake typed NUMBER/DATE/TIMESTAMP columns.
  - Only use `safe_to_number()` if the input is STRING/VARIANT and the prompt says so.
- Never reference columns not present in the specified source table.
- Generate deterministic SQL (no random()).

## Implementation pattern for each staging model
1) Use a CTE that calls dedupe macro:
   - `{{ dedupe_latest(source(source_name, source_table), primary_key, updated_at) }}`
2) Select only requested columns.
3) Apply enum normalization macros where provided:
   - `{{ <macro>(<col>) }} as <col>`
4) Leave typed columns as-is unless prompt indicates raw is string/variant.

## Files to create/update
- `models/staging/<staging_model>.sql` for each entry
- `models/staging/schema.yml` (add tests for all generated staging models)

## Tests to include (safe defaults)
- `unique` + `not_null` for primary_key columns (post-dedupe)
- For enum columns: use `accepted_values_or_unknown` if available, else `accepted_values` with `severity: warn`
- Relationships: `severity: warn`

## Self-check loop
After generation, recommend these commands (do not run unless asked):
- `dbt parse`
- `dbt build -s <generated_staging_models> --fail-fast`

If the caller pastes errors:
- First check macro call signatures and SQL compilation
- Apply minimal diffs to fix
- Re-run the smallest selector first

---
name: dbt_build_incremental_facts_for_demo
description: Build incremental fact models from staging. Merge-safe, deterministic, minimal transformations. Reads staging models to confirm column availability before generating SQL.
tools: []
---

## Goal
Create incremental fact models in `models/marts/{source_name}/` that:
1. Select from the staging models created by skill `02_generate_staging`
2. Add light business logic (calculated fields only — no heavy transforms)
3. Are incremental with merge strategy for idempotent reruns
4. Are deterministic — same input always produces same output

## Required Inputs (ask if missing)
| Parameter       | Description                                    | Example         |
|-----------------|------------------------------------------------|-----------------|
| `source_name`   | dbt source name (used in staging model names)  | `raw_acq`       |

Before generating any SQL, **read the staging model files and `_staging_tests.yml`** from skill 02 to confirm:
- Which staging models exist and their exact names
- Which columns are available in each staging model
- What the PK column is for each model

Do NOT assume column names or staging model names — only reference what exists.

## Hard Rules (do not violate)

### Incremental Configuration
- Every fact model MUST use this config block:
  ```sql
  {{
      config(
          materialized='incremental',
          incremental_strategy='merge',
          unique_key='{pk_column}'
      )
  }}
  ```
- The `unique_key` must match the grain of the fact:
  - `f_policy`: `policy_id`
  - `f_claim`: `claim_id`
  - `f_claim_txn`: `claim_txn_id`

### Incremental Filter
- Every fact model MUST include this filter for incremental runs:
  ```sql
  {% if is_incremental() %}
      where updated_at > (select max(updated_at) from {{ this }})
  {% endif %}
  ```
- If the project has an `incremental_where` macro, use it: `{{ incremental_where('updated_at') }}`
- If unsure whether the macro exists, use the inline `{% if is_incremental() %}` pattern — it always works.

### Type Casting Rules (same as staging — minimal)
- **DEFAULT: No casting.** Staging already handled normalization.
- **Timestamps**: Pass through as-is. No casting.
- **IDs**: Pass through as-is. No casting.
- **Amounts**: Already numeric from staging. Pass through as-is.
- **Calculated fields only**: The only new expressions should be simple arithmetic or `datediff` — nothing that could fail on NULLs without a `coalesce`.

### Determinism
- No `order by random()`, `uuid_generate()`, `current_timestamp()` in SELECT columns, or `sample()`.
- `updated_at` passes through from staging — do NOT replace it with `current_timestamp()`.

### Ref Names
Use the exact staging model names from skill 02. With the recommended naming convention:
- `{{ ref('stg_{source_name}__customer') }}`
- `{{ ref('stg_{source_name}__policy') }}`
- `{{ ref('stg_{source_name}__claim') }}`
- `{{ ref('stg_{source_name}__claim_txn') }}`

**IMPORTANT:** Confirm these names match the actual files before writing SQL. If staging used a different naming convention, match that.

### File Placement
- SQL models go in `models/marts/{source_name}/`
- Schema YAML goes in `models/marts/{source_name}/_marts_tests.yml`

## Fact Models to Generate

### f_policy
**Source:** `stg_{source_name}__policy` joined to `stg_{source_name}__customer`
**Grain:** One row per `policy_id`
**Join type:** `left join` on `customer_id` (some policies may have orphaned customer references)

| Output Column      | Source                         | Treatment                          |
|--------------------|--------------------------------|------------------------------------|
| `policy_id`        | policy.policy_id               | Pass through (PK)                  |
| `policy_number`    | policy.policy_number           | Pass through                       |
| `customer_id`      | policy.customer_id             | Pass through (FK)                  |
| `customer_name`    | customer.customer_name         | Pass through from join (nullable)  |
| `line_of_business` | policy.line_of_business        | Pass through (normalized in stg)   |
| `carrier`          | policy.carrier                 | Pass through                       |
| `policy_status`    | policy.policy_status           | Pass through (normalized in stg)   |
| `effective_date`   | policy.effective_date          | Pass through (no cast)             |
| `expiration_date`  | policy.expiration_date         | Pass through (no cast)             |
| `written_premium`  | policy.written_premium         | Pass through (numeric from stg)    |
| `policy_term_days` | calculated                     | `datediff('day', policy.effective_date, policy.expiration_date)` |
| `updated_at`       | policy.updated_at              | Pass through (no cast)             |

**Null safety for calculated fields:**
```sql
case
    when policy.effective_date is not null and policy.expiration_date is not null
    then datediff('day', policy.effective_date, policy.expiration_date)
    else null
end as policy_term_days
```

### f_claim
**Source:** `stg_{source_name}__claim` (no joins needed)
**Grain:** One row per `claim_id`

| Output Column        | Source                    | Treatment                          |
|----------------------|---------------------------|------------------------------------|
| `claim_id`           | claim.claim_id            | Pass through (PK)                  |
| `policy_id`          | claim.policy_id           | Pass through (FK)                  |
| `loss_date`          | claim.loss_date           | Pass through (no cast)             |
| `reported_date`      | claim.reported_date       | Pass through (no cast)             |
| `claim_status`       | claim.claim_status        | Pass through (normalized in stg)   |
| `incurred_amount`    | claim.incurred_amount     | Pass through (numeric from stg)    |
| `paid_amount`        | claim.paid_amount         | Pass through (numeric from stg)    |
| `incurred_minus_paid`| calculated                | `coalesce(claim.incurred_amount, 0) - coalesce(claim.paid_amount, 0)` |
| `days_to_report`     | calculated                | See null-safe pattern below        |
| `updated_at`         | claim.updated_at          | Pass through (no cast)             |

**Null safety:**
```sql
coalesce(incurred_amount, 0) - coalesce(paid_amount, 0) as incurred_minus_paid,

case
    when loss_date is not null and reported_date is not null
    then datediff('day', loss_date, reported_date)
    else null
end as days_to_report
```

### f_claim_txn
**Source:** `stg_{source_name}__claim_txn` (no joins needed)
**Grain:** One row per `claim_txn_id`

| Output Column      | Source                       | Treatment                          |
|--------------------|------------------------------|------------------------------------|
| `claim_txn_id`     | claim_txn.claim_txn_id       | Pass through (PK)                  |
| `claim_id`         | claim_txn.claim_id           | Pass through (FK)                  |
| `txn_type`         | claim_txn.txn_type           | Pass through (normalized in stg)   |
| `txn_date`         | claim_txn.txn_date           | Pass through (no cast)             |
| `txn_amount`       | claim_txn.txn_amount         | Pass through (numeric from stg)    |
| `txn_sign`         | calculated                   | See pattern below                  |
| `updated_at`       | claim_txn.updated_at         | Pass through (no cast)             |

**txn_sign logic:**
```sql
case
    when coalesce(txn_amount, 0) >= 0 then 'positive'
    else 'negative'
end as txn_sign
```

## Model SQL Pattern (follow exactly)

```sql
-- models/marts/{source_name}/f_{entity}.sql

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='{pk_column}'
    )
}}

with {entity} as (
    select * from {{ ref('stg_{source_name}__{entity}') }}
    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

-- add additional CTEs for joins if needed (f_policy joins customer)

select
    -- keys
    ...
    -- dimensions (pass through)
    ...
    -- amounts (pass through)
    ...
    -- calculated fields (null-safe)
    ...
    -- timestamps (pass through)
    ...
from {entity}
-- left join if needed
```

### Pattern Notes
- The incremental filter goes in the FIRST CTE (the one reading from staging), not at the end.
- For `f_policy`, add a second CTE for `customer` and left join in the final select.
- Calculated fields MUST be null-safe using `coalesce` or `case when ... is not null`.
- The final `select` lists columns explicitly — no `*`.

## Schema Tests (`_marts_tests.yml`)

Keep tests minimal:

```yaml
version: 2

models:
  - name: f_policy
    columns:
      - name: policy_id
        tests:
          - not_null
          - unique

  - name: f_claim
    columns:
      - name: claim_id
        tests:
          - not_null
          - unique
      - name: policy_id
        tests:
          - relationships:
              to: ref('f_policy')
              field: policy_id
              severity: warn

  - name: f_claim_txn
    columns:
      - name: claim_txn_id
        tests:
          - not_null
          - unique
      - name: claim_id
        tests:
          - relationships:
              to: ref('f_claim')
              field: claim_id
              severity: warn
```

### Test Rules
- `unique` + `not_null` on every PK → `severity: error`
- `relationships` on FKs → `severity: warn` (orphaned keys expected in acquisition data)
- Do NOT add `not_null` on FK columns — orphaned keys are real in acquired data
- Do NOT add accepted_values, expression, or rowcount tests

## Error Recovery
If `dbt build -s marts` fails:
1. **Compilation error**: check `ref()` names match actual staging model filenames
2. **Database error — column not found**: a staging model doesn't output that column. Read the staging SQL to confirm column names.
3. **Database error — type mismatch in datediff/arithmetic**: a column is NULL or wrong type. Add `coalesce()` wrapper or `case when is not null` guard.
4. **Merge conflict**: check `unique_key` matches the actual grain. Run `--full-refresh` to reset.
5. **Test failure at error**: PK test failed — dedupe issue in staging. Fix upstream.
6. **Test failure at warn**: FK relationship mismatch — expected for acquisitions. Proceed.

For a clean reset: `dbt build -s marts --full-refresh`

## When Done
1. Run: `dbt build -s marts`
2. If first run, this is equivalent to a full-refresh (table doesn't exist yet)
3. Report results — passes, warns, and any errors
4. Provide the full-refresh command for demo resets: `dbt build -s marts --full-refresh`
5. Suggest showing the lineage graph: raw sources → staging → marts

## What NOT to Do
- Do NOT modify staging models or source definitions
- Do NOT modify `dbt_project.yml` or macros
- Do NOT cast timestamps or IDs
- Do NOT use `current_timestamp()` as `updated_at` — always pass through from staging
- Do NOT use nondeterministic functions
- Do NOT add complex business logic — this is a thin fact layer for the demo
- Do NOT add `not_null` tests on FK columns
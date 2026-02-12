---
name: dbt_bootstrap_sources_snowflake
description: Discover and define dbt sources + schema tests for any Snowflake raw schema. Dynamically introspects tables and columns — works for any acquired entity.
tools: []
---

## Goal
Given a Snowflake database and schema containing raw acquisition data, **discover** what's there and create:
1. `models/staging/{source_name}/_sources.yml` — source definitions with all tables and columns
2. `models/staging/{source_name}/_sources_tests.yml` — safe integrity tests
3. `models/staging/{source_name}/README.md` — assumptions, known issues, column mapping notes

## Required Inputs (ask if missing)
| Parameter       | Description                          | Example                    |
|-----------------|--------------------------------------|----------------------------|
| `database`      | Snowflake database                   | `AG_DEMO`                  |
| `schema`        | Snowflake schema with raw tables     | `RAW_ACQ`                  |
| `source_name`   | dbt source name (snake_case)         | `raw_acq` or `raw_acq_ap`  |
| `entity_label`  | Human-readable acquisition name      | `AssuredPartners Export Q3` |
| `tables`        | (Optional) Specific tables to include. If omitted, **discover all tables in the schema** via: `SELECT table_name FROM {database}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' ORDER BY table_name` |

## Hard Rules (do not violate)
- Use `snake_case` for ALL dbt model names, column names, and source names.
- Do NOT write custom SQL, business logic, or transformation code.
- Do NOT create any `.sql` model files in this skill — sources only.
- Add tests only where they won't be flaky:
  - NO rowcount tests
  - NO exact value assertions on data that varies by acquisition
- ALL relationship tests MUST use `severity: warn` (acquired data has orphaned keys).
- ALL accepted_values tests MUST use `severity: warn` (code values vary by source system).
- Output files go in `models/staging/{source_name}/` — NOT in the project root.

## Step 1: Discover Tables
If tables are not provided, query `INFORMATION_SCHEMA.TABLES`:
```sql
Discover tables using dbt Cloud CLI:
  dbt show --inline "SELECT table_name FROM {database}.INFORMATION_SCHEMA.TABLES WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'" --limit 100

Discover columns per table using:
  dbt show --inline "SELECT column_name, data_type, is_nullable FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{schema}' AND table_name = '{table}'" --limit 500
```
Convert all column names to `snake_case` in the YAML output.

## Step 3: Identify Primary Keys (Heuristic)
Apply these rules in order to identify the likely primary key column for each table:
1. Column named exactly `{table_name_without_prefix}_id` (e.g., table `RAW_CUSTOMER` → `customer_id`)
2. Column named `id`
3. First column ending in `_id`
4. If none found, flag as "⚠️ no obvious PK — manual review needed"

## Step 4: Generate `_sources.yml`
```yaml
version: 2

sources:
  - name: {source_name}
    database: {database}
    schema: {schema}
    description: >
      Raw data export from acquired entity: {entity_label}.
      Loaded as-is — no transformations applied.
      Known issues: inconsistent code values, potential duplicates,
      null names/fields, late-arriving updates, orphaned foreign keys.
    freshness:
      warn_after:
        count: 24
        period: hour
      error_after:
        count: 72
        period: hour
    loaded_at_field: "coalesce(updated_at, created_at, current_timestamp())"
    tables:
      - name: {table_name_snake_case}
        description: "Raw {entity_label} — {table description}"
        columns:
          - name: {column_name_snake_case}
            description: "{inferred description}"
```

### Column Description Heuristics
- `*_id` → "Primary/foreign key identifier"
- `*_date`, `*_at` → "Timestamp field"
- `*_amount`, `*_premium`, `*_cost` → "Monetary amount (raw, unvalidated)"
- `*_status`, `*_type`, `*_code` → "Code/status value (may need normalization)"
- `*_name` → "Name field (may contain nulls in acquired data)"
- Everything else → "Raw field — review for business meaning"

## Step 5: Generate `_sources_tests.yml`
```yaml
version: 2

sources:
  - name: {source_name}
    tables:
      - name: {table}
        columns:
          - name: {pk_column}
            tests:
              - not_null
              - unique
```

### Test Rules
| Test Type         | When to Apply                                         | Severity |
|-------------------|-------------------------------------------------------|----------|
| `not_null`        | On every identified PK column                         | `error`  |
| `unique`          | On every identified PK column                         | `error`  |
| `not_null`        | On FK columns (`*_id` that reference another table)   | `warn`   |
| `relationships`   | FK → PK of the parent table (only if parent is in same source) | `warn` |
| `accepted_values` | Only if a known code set exists AND skill is told the values | `warn` |

### Relationship Detection Heuristic
For each non-PK column ending in `_id`, check if a table exists in the same source whose PK matches:
- `customer_id` → look for table with PK `customer_id`
- `policy_id` → look for table with PK `policy_id`
If found, add a `relationships` test with `severity: warn`.

## Step 6: Generate `README.md`
```markdown
# Source: {source_name}
## Entity: {entity_label}
## Schema: {database}.{schema}

### Tables Discovered
| Table | Rows (approx) | PK Column | Notes |
|-------|---------------|-----------|-------|

### Known Issues (Standard for Acquisitions)
- Code values may not match Gallagher standards — normalization handled in staging
- Duplicate records possible — deduplication handled in staging
- Null names and optional fields expected
- Late-arriving updates may cause key mismatches
- Foreign key integrity NOT guaranteed across tables

### Next Steps
- [ ] Review column descriptions and correct business meaning
- [ ] Confirm PK assumptions
- [ ] Run `dbt build -s source:{source_name}` to validate
- [ ] Confirm proceeding to staging skill (02_generate_staging) with user
```

## When Done
Provide:
1. Summary of tables discovered and columns mapped
2. List of any warnings (no PK found, unexpected data types, etc.)
3. Commands to validate:
   ```bash
   dbt parse
   dbt build -s source:{source_name}
   ```
4. Recommendation to proceed to skill `02_generate_staging`

## What NOT to Do
- Do NOT modify `dbt_project.yml`
- Do NOT modify existing macros
- Do NOT create SQL model files
- Do NOT assume column existence — always verify via INFORMATION_SCHEMA
- Do NOT add tests on columns you haven't confirmed exist
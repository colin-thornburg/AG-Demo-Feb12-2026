### Sources ###
Use skill .agents/skills/01_bootstrap_sources/SKILL.md exactly as written.

Inputs:
- database: AG_DEMO
- schema: RAW_ACQ
- source_name: raw_acq
- entity_label: AssuredPartners Southeast Region

Discover all tables in AG_DEMO.RAW_ACQ using INFORMATION_SCHEMA.
For each table, discover all columns using INFORMATION_SCHEMA.COLUMNS.
Identify primary keys using the skill's heuristic rules.

Generate:
1. models/staging/raw_acq/_sources.yml
2. models/staging/raw_acq/_sources_tests.yml  
3. models/staging/raw_acq/README.md

Follow every Hard Rule in the skill. Do not deviate.
Do not modify dbt_project.yml or any macros.
When done, output the validation commands.


### Staging ###
Use skill .agents/skills/02_generate_staging/SKILL.md exactly as written.

First, read models/staging/raw_acq/_sources.yml to confirm which tables 
and columns exist. Do not assume — only reference columns that are defined 
in the sources YAML.

Inputs:
- source_name: raw_acq
- source_path: models/staging/raw_acq/

Generate staging models for all 4 source tables:
- stg_raw_acq__customer.sql
- stg_raw_acq__policy.sql
- stg_raw_acq__claim.sql
- stg_raw_acq__claim_txn.sql

And generate: models/staging/raw_acq/_staging_tests.yml

CRITICAL RULES — read these before writing any SQL:
- Do NOT cast any timestamp columns. Pass created_at, updated_at, 
  effective_date, expiration_date, loss_date, reported_date, txn_date 
  through as-is.
- Do NOT cast any ID columns. Pass through as-is.
- Use safe_to_number() ONLY on amount columns that are VARCHAR type 
  in the source. If they're already NUMBER, pass through.
- Use the normalize macros for status/code fields. Do NOT write 
  inline CASE statements.
- Use dedupe_latest macro on every model.
- Tests: unique + not_null on PKs only. Relationships at warn severity. 
  No accepted_values tests.

Follow every Hard Rule in the skill. Do not deviate.
Do not modify _sources.yml, dbt_project.yml, or any macros.

When done, run: dbt build -s staging
If any errors occur, read the error, apply the smallest fix, and re-run.

### Marts ###
Use skill .agents/skills/03_build_facts_incremental/SKILL.md exactly as written.

First, read the staging model SQL files in models/staging/raw_acq/ to 
confirm the exact model names and which columns each staging model outputs. 
Do not assume — only reference columns and ref() names that actually exist.

Inputs:
- source_name: raw_acq

Generate 3 incremental fact models:
- models/marts/raw_acq/f_policy.sql
- models/marts/raw_acq/f_claim.sql
- models/marts/raw_acq/f_claim_txn.sql

And generate: models/marts/raw_acq/_marts_tests.yml

CRITICAL RULES — read these before writing any SQL:

1. INCREMENTAL CONFIG: Every model must use materialized='incremental', 
   incremental_strategy='merge', and define unique_key.

2. INCREMENTAL FILTER: Use {% if is_incremental() %} where updated_at > 
   (select max(updated_at) from {{ this }}) {% endif %} in the first CTE. 
   If the incremental_where macro exists in the project, use that instead.

3. NO CASTING: Staging already cleaned the data. Pass through all columns 
   as-is. No timestamp casts, no ID casts, no amount casts.

4. NULL-SAFE CALCULATIONS: Every calculated field (policy_term_days, 
   incurred_minus_paid, days_to_report, txn_sign) must handle NULLs 
   with coalesce() or case-when-is-not-null guards.

5. JOINS: f_policy left joins to stg_raw_acq__customer on customer_id. 
   Use LEFT JOIN — not inner join — because customer references may be 
   orphaned in acquired data.

6. TESTS: unique + not_null on PKs only (error severity). Relationships 
   on FKs at warn severity. No not_null on FK columns. No accepted_values.

7. REF NAMES: Use the exact staging model names:
   - ref('stg_raw_acq__customer')
   - ref('stg_raw_acq__policy')
   - ref('stg_raw_acq__claim')
   - ref('stg_raw_acq__claim_txn')
   Verify these match the actual filenames before writing SQL.

Follow every Hard Rule in the skill. Do not deviate.
Do not modify staging models, sources, dbt_project.yml, or any macros.

When done, run: dbt build -s marts
If this is the first run, it will create the tables (equivalent to full-refresh).
Report results. If errors, read the error, apply smallest fix, re-run.


### auditor ###
---
name: dbt_project_auditor
description: Audit a dbt project against our company's Blueprint standards and generate a polished HTML report. Runs as the final demo step to prove AI-generated code follows COE governance.
tools: []
---

## Goal
Scan the current dbt project and produce a single-file HTML audit report that checks compliance with our data engineering standards. The report should be visually polished, executive-ready, and openable locally in a browser.

## Required Inputs (ask if missing)
| Parameter       | Description                                    | Example         |
|-----------------|------------------------------------------------|-----------------|
| `source_name`   | dbt source name being audited                  | `raw_acq`       |
| `entity_label`  | Human-readable acquisition name                | `AssuredPartners Demo` |
| `output_path`   | Where to save the HTML report                  | `reports/audit_report.html` |

## Audit Checks to Perform

Read the project files and evaluate each check as PASS, WARN, or FAIL.

### 1. Naming Conventions
Scan all `.sql` files in `models/` and all `.yml` files:
- [ ] All model filenames use `snake_case` (no camelCase, no hyphens)
- [ ] Staging models follow pattern: `stg_{source_name}__*.sql`
- [ ] Mart/fact models follow pattern: `f_*.sql`
- [ ] Source name in `_sources.yml` is `snake_case`
- [ ] All column names in YAML are `snake_case`

### 2. Project Structure
Check directory layout:
- [ ] Sources YAML is in `models/staging/{source_name}/`
- [ ] Staging SQL is in `models/staging/{source_name}/`
- [ ] Mart SQL is in `models/marts/{source_name}/` or `models/marts/`
- [ ] No SQL files in the project root `models/` directory
- [ ] No orphaned SQL files outside the expected directories

### 3. Source Definitions
Read `_sources.yml`:
- [ ] Every raw table has a source definition
- [ ] Source has a `description` field (not empty)
- [ ] Source has `freshness` configuration
- [ ] All tables have at least one documented column

### 4. Staging Standards
For each staging model, read the SQL:
- [ ] Uses `source()` function (not hardcoded table references)
- [ ] Uses `dedupe_latest` macro (not inline ROW_NUMBER)
- [ ] Uses normalization macros where applicable (not inline CASE)
- [ ] Does NOT cast timestamps (no `try_to_timestamp`, `to_timestamp`, `cast(... as timestamp)`)
- [ ] Does NOT use `SELECT *` in the final select (explicit column list)
- [ ] Has explicit column aliasing

### 5. Mart/Fact Standards
For each mart model, read the SQL:
- [ ] Uses `ref()` function (not hardcoded table names)
- [ ] Has `materialized='incremental'` config
- [ ] Has `incremental_strategy='merge'` config
- [ ] Has `unique_key` defined
- [ ] Has incremental filter (`is_incremental()` block)
- [ ] Uses LEFT JOIN (not INNER JOIN) for dimension lookups
- [ ] Calculated fields are null-safe (use `coalesce` or `case when`)

### 6. Testing Coverage
Read all `_*_tests.yml` and `schema.yml` files:
- [ ] Every primary key column has `not_null` + `unique` tests
- [ ] Foreign key relationships use `severity: warn`
- [ ] No flaky tests (no rowcount, no exact-value assertions)
- [ ] Test count: at minimum 2 tests per model (PK tests)

### 7. Documentation
- [ ] Source description exists and mentions acquisition context
- [ ] README or doc file exists describing known data issues
- [ ] At least 50% of columns in YAML have descriptions

### 8. Macro Usage (No Inline Logic)
Scan all staging SQL for anti-patterns:
- [ ] No inline `CASE WHEN` blocks longer than 3 lines (should use normalization macros)
- [ ] No inline `ROW_NUMBER()` window functions (should use `dedupe_latest`)
- [ ] No `TRY_TO_*` cast functions in staging models

## Report Generation

Generate a single HTML file with embedded CSS and JavaScript. The report must:

1. **Work offline** — no external CDN dependencies, all styles inline
2. **Be visually polished** — dark theme, Gallagher navy (#1B2A4A) + dbt orange (#FF694A) color scheme
3. **Show a summary dashboard** at the top with:
   - Overall score (% of checks passing)
   - Pass / Warn / Fail counts with colored badges
   - Project name, entity label, audit timestamp
4. **Show each audit category** as a collapsible section with:
   - Category name and pass rate
   - Individual check results with PASS (green ✓), WARN (yellow ⚠), or FAIL (red ✕)
   - For failures: specific file and line reference plus remediation guidance
5. **Include a "Standards Compliance" summary** at the bottom:
   - "This project was generated by AI using dbt Agent Skills"
   - "Audited against Gallagher Blueprint v1.0 standards"
   - Timestamp of audit

### HTML Template Structure

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>dbt Project Audit — {entity_label}</title>
    <style>
        /* DESIGN REQUIREMENTS:
         * - Dark background: #0F172A (deep navy)
         * - Cards: #1E293B
         * - Accent: #FF694A (dbt orange)
         * - Pass: #22C55E (green)
         * - Warn: #F59E0B (amber)  
         * - Fail: #EF4444 (red)
         * - Text: #E2E8F0 (light gray)
         * - Font: system-ui with monospace for file paths
         * - Subtle border-radius on cards
         * - Smooth transitions on collapsible sections
         * - Responsive — works on any screen width
         */
    </style>
</head>
<body>
    <!-- HEADER: Project name, entity, timestamp, overall score -->
    <!-- SUMMARY CARDS: Total checks, Pass count, Warn count, Fail count -->
    <!-- AUDIT CATEGORIES: Collapsible sections with individual checks -->
    <!-- FOOTER: Standards version, generation note -->
</body>
</html>
```

### Visual Design Specifications

**Overall Score Display:**
- Large circular progress indicator or percentage number
- Color: green if >90%, amber if 70-90%, red if <70%
- Font size: 48px+ for the number

**Category Cards:**
- Dark card (#1E293B) on dark background (#0F172A)
- Left border accent: green if all pass, amber if warns, red if fails
- Click to expand/collapse individual checks
- Category pass rate shown as a small badge

**Individual Check Results:**
- ✓ PASS — green (#22C55E) with subtle green-tinted background
- ⚠ WARN — amber (#F59E0B) with subtle amber-tinted background
- ✕ FAIL — red (#EF4444) with subtle red-tinted background
- File paths in monospace font
- Remediation text in smaller, muted font below failures

**Footer:**
- "Audited against Gallagher Blueprint v1.0"
- "Generated by dbt Agent Skills"
- Timestamp
- dbt Labs × Gallagher branding

## How to Perform the Audit

### Step 1: Inventory the project
```bash
find models/ -name "*.sql" -o -name "*.yml" | sort
```
List all model and YAML files.

### Step 2: Read each file
For every `.sql` file, read the contents and check against the relevant standards.
For every `.yml` file, read and validate structure and content.

### Step 3: Score each check
- **PASS**: requirement is fully met
- **WARN**: partially met or minor deviation (e.g., missing description but structure is correct)
- **FAIL**: requirement is violated (e.g., inline CASE instead of macro, no tests on PK)

### Step 4: Generate the HTML
Build the complete HTML string with all results embedded. Write to `{output_path}`.

### Step 5: Report
```
Audit complete. Report saved to: {output_path}
Open in browser: open {output_path}

Summary: {pass_count} passed, {warn_count} warnings, {fail_count} failures
Overall score: {score}%
```

## Error Handling
- If a file can't be read, mark its checks as WARN with note "file not accessible"
- If a directory doesn't exist (e.g., no marts yet), mark those checks as WARN with note "directory not found — run skill 03 first"
- Never crash — the audit must always produce a report, even if incomplete

## What NOT to Do
- Do NOT modify any project files — this is read-only
- Do NOT run dbt commands — this is a static analysis only
- Do NOT require any external dependencies (no npm, no pip)
- Do NOT use external CDN links in the HTML — everything must be inline
- Do NOT produce a partial report — always generate the full HTML even if some checks can't run
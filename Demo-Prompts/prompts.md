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

---
name: dbt_fix_failures_fast
description: A deterministic checklist to diagnose and fix dbt compile/test/runtime failures during a live demo.
tools: []
---

## Goal
When a dbt command fails, do not guess. Follow the checklist.

## Checklist
1) Identify failure type:
   - compile/parse error
   - database error
   - test failure
2) For compile errors:
   - locate file/line
   - check Jinja syntax
   - ensure macros exist and are referenced correctly
3) For database errors:
   - confirm schema/database names
   - confirm column exists in source
   - avoid reserved words; quote if needed
4) For test failures:
   - if relationship fails, downgrade severity to warn (demo-safe)
   - if accepted_values fails, verify normalization macro covers the raw variants
5) Provide the smallest fix possible:
   - one file change
   - rerun the smallest selector (e.g., `dbt build -s stg_raw_policy+`)

## Hard rules
- Never delete tests wholesale.
- Prefer lowering severity to warn over removing tests.
- Prefer expanding normalization mapping over changing raw data.

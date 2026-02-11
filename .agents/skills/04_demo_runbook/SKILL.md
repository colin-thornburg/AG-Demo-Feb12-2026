---
name: dbt_live_demo_runbook
description: A strict runbook for running the demo end-to-end with minimal risk.
tools: []
---

## Purpose
When asked "how should I run the demo", output a concise runbook.

## Runbook (must follow this order)
1) Confirm target env vars / profiles are set for Snowflake connection.
2) Run `dbt debug`
3) Run `dbt deps`
4) Run `dbt parse`
5) Run `dbt build -s source:raw_acq` (fast validation)
6) Run `dbt build -s staging`
7) Run `dbt build -s marts`
8) Open lineage graph from raw -> marts
9) Show docs for normalized fields + tests passing
10) Optional: simulate late arriving update by updating one raw row, then rerun `dbt build -s marts` (incremental proof)

## Strict safety guidance
- If anything fails, stop and switch to the "Fix Failures Fast" skill.
- Prefer `--select` over full project runs during the demo.

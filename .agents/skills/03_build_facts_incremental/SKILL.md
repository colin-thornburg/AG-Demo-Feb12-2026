---
name: dbt_build_incremental_facts_for_demo
description: Build incremental fact models that are merge-safe and deterministic for live demos.
tools: []
---

## Goal
Create 3 incremental fact models in `models/marts/`:
- `f_policy`
- `f_claim`
- `f_claim_txn`

## Hard rules (no live-demo surprises)
- Must be `materialized='incremental'` with `incremental_strategy='merge'`
- Must define `unique_key` at the grain:
  - f_policy: policy_id
  - f_claim: claim_id
  - f_claim_txn: claim_txn_id
- Incremental filter MUST use macro `{{ incremental_where('updated_at') }}` so it works on full-refresh and incremental.
- Never use nondeterministic SQL (no `order by random()`).

## Inputs
Use only staging models:
- `ref('stg_raw_policy')`, `ref('stg_raw_claim')`, `ref('stg_raw_claim_txn')`, `ref('stg_raw_customer')`

## Required columns (keep stable)
### f_policy
- policy_id, policy_number, customer_id
- line_of_business, carrier, policy_status
- effective_date, expiration_date
- written_premium
- policy_term_days (datediff)
- updated_at

### f_claim
- claim_id, policy_id
- loss_date, reported_date
- claim_status
- incurred_amount, paid_amount
- incurred_minus_paid
- updated_at

### f_claim_txn
- claim_txn_id, claim_id
- txn_type, txn_date, txn_amount
- txn_sign (positive/negative)
- updated_at

## Tests
Create `models/marts/schema.yml`:
- unique/not_null for ids
- relationships:
  - f_claim.policy_id -> f_policy.policy_id (warn)
  - f_claim_txn.claim_id -> f_claim.claim_id (warn)

## When done
Provide run commands:
- `dbt build -s marts`
- `dbt build -s marts --full-refresh` (for reset)

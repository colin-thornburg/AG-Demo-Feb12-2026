# Source: raw_acq
## Entity: AssuredPartners Southeast Region
## Schema: AG_DEMO.RAW_ACQ

### Tables Discovered
| Table | Rows (approx) | PK Column | Notes |
|-------|---------------|-----------|-------|
| raw_claim | N/A (not profiled) | claim_id | PK inferred via `{table_name_without_prefix}_id` heuristic |
| raw_claim_txn | N/A (not profiled) | claim_txn_id | PK inferred via `{table_name_without_prefix}_id` heuristic |
| raw_customer | N/A (not profiled) | customer_id | PK inferred via `{table_name_without_prefix}_id` heuristic |
| raw_policy | N/A (not profiled) | policy_id | PK inferred via `{table_name_without_prefix}_id` heuristic |

### Known Issues (Standard for Acquisitions)
- Code values may not match Gallagher standards — normalization handled in staging
- Duplicate records possible — deduplication handled in staging
- Null names and optional fields expected
- Late-arriving updates may cause key mismatches
- Foreign key integrity NOT guaranteed across tables

### Next Steps
- [ ] Review column descriptions and correct business meaning
- [ ] Confirm PK assumptions
- [ ] Run `dbt build -s source:raw_acq` to validate
- [ ] Confirm proceeding to staging skill (02_generate_staging) with user

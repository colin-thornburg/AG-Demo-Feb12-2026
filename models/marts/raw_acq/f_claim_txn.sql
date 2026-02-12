{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='claim_txn_id'
    )
}}

with claim_txn as (
    select * from {{ ref('stg_raw_acq__claim_txn') }}
    {{ incremental_where('updated_at') }}
)

select
    claim_txn.claim_txn_id,
    claim_txn.claim_id,
    claim_txn.txn_type,
    claim_txn.txn_date,
    claim_txn.txn_amount,
    case
        when coalesce(claim_txn.txn_amount, 0) >= 0 then 'positive'
        else 'negative'
    end as txn_sign,
    claim_txn.updated_at
from claim_txn

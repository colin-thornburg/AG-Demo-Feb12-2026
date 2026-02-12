with source as (
    select * from {{ source('raw_acq', 'raw_claim_txn') }}
),

deduped as (
    {{ dedupe_latest('source', ['claim_txn_id'], 'updated_at') }}
),

renamed as (
    select
        claim_txn_id,
        claim_id,
        {{ normalize_txn_type('txn_type') }} as txn_type,
        txn_date,
        txn_amount,
        updated_at
    from deduped
)

select
    claim_txn_id,
    claim_id,
    txn_type,
    txn_date,
    txn_amount,
    updated_at
from renamed

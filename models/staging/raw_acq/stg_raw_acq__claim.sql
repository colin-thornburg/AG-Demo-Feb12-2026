with source as (
    select * from {{ source('raw_acq', 'raw_claim') }}
),

deduped as (
    {{ dedupe_latest('source', ['claim_id'], 'updated_at') }}
),

renamed as (
    select
        claim_id,
        policy_id,
        loss_date,
        reported_date,
        {{ normalize_claim_status('claim_status') }} as claim_status,
        incurred_amount,
        paid_amount,
        updated_at
    from deduped
)

select
    claim_id,
    policy_id,
    loss_date,
    reported_date,
    claim_status,
    incurred_amount,
    paid_amount,
    updated_at
from renamed

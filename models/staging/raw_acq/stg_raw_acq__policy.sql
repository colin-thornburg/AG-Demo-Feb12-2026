with source as (
    select * from {{ source('raw_acq', 'raw_policy') }}
),

deduped as (
    {{ dedupe_latest('source', ['policy_id'], 'updated_at') }}
),

renamed as (
    select
        policy_id,
        policy_number,
        customer_id,
        {{ normalize_lob('line_of_business') }} as line_of_business,
        carrier,
        effective_date,
        expiration_date,
        written_premium,
        {{ normalize_policy_status('policy_status') }} as policy_status,
        updated_at
    from deduped
)

select
    policy_id,
    policy_number,
    customer_id,
    line_of_business,
    carrier,
    effective_date,
    expiration_date,
    written_premium,
    policy_status,
    updated_at
from renamed

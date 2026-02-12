with source as (
    select * from {{ source('raw_acq', 'raw_customer') }}
),

deduped as (
    {{ dedupe_latest('source', ['customer_id'], 'updated_at') }}
),

renamed as (
    select
        customer_id,
        customer_name,
        customer_type,
        state,
        created_at,
        updated_at
    from deduped
)

select
    customer_id,
    customer_name,
    customer_type,
    state,
    created_at,
    updated_at
from renamed

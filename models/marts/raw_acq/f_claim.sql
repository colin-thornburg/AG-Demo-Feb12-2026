{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='claim_id'
    )
}}

with claim as (
    select * from {{ ref('stg_raw_acq__claim') }}
    {{ incremental_where('updated_at') }}
)

select
    claim.claim_id,
    claim.policy_id,
    claim.loss_date,
    claim.reported_date,
    claim.claim_status,
    claim.incurred_amount,
    claim.paid_amount,
    coalesce(claim.incurred_amount, 0) - coalesce(claim.paid_amount, 0) as incurred_minus_paid,
    case
        when claim.loss_date is not null and claim.reported_date is not null
            then datediff('day', claim.loss_date, claim.reported_date)
        else null
    end as days_to_report,
    claim.updated_at
from claim

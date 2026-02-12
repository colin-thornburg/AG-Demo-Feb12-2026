{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='policy_id'
    )
}}

with policy as (
    select * from {{ ref('stg_raw_acq__policy') }}
    {{ incremental_where('updated_at') }}
),

customer as (
    select * from {{ ref('stg_raw_acq__customer') }}
)

select
    policy.policy_id,
    policy.policy_number,
    policy.customer_id,
    customer.customer_name,
    policy.line_of_business,
    policy.carrier,
    policy.policy_status,
    policy.effective_date,
    policy.expiration_date,
    policy.written_premium,
    case
        when policy.effective_date is not null and policy.expiration_date is not null
            then datediff('day', policy.effective_date, policy.expiration_date)
        else null
    end as policy_term_days,
    policy.updated_at
from policy
left join customer
    on policy.customer_id = customer.customer_id

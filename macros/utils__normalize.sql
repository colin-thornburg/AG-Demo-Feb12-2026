{% macro normalize_policy_status(col) %}
case
  when {{ col }} is null then null
  when lower(trim({{ col }})) in ('active') then 'active'
  when lower(trim({{ col }})) in ('cancelled','canceled') then 'cancelled'
  when lower(trim({{ col }})) in ('expired') then 'expired'
  else 'unknown'
end
{% endmacro %}

{% macro normalize_lob(col) %}
case
  when {{ col }} is null then null
  when lower(trim({{ col }})) in ('p&c','pc','p_c','propertycasualty','property_casualty') then 'pc'
  when lower(trim({{ col }})) in ('ben','benefits','employeebenefits','employee_benefits') then 'benefits'
  when lower(trim({{ col }})) in ('cyber','cyb') then 'cyber'
  else 'unknown'
end
{% endmacro %}

{% macro normalize_claim_status(col) %}
case
  when {{ col }} is null then null
  when lower(trim({{ col }})) in ('open') then 'open'
  when lower(trim({{ col }})) in ('closed') then 'closed'
  when lower(trim({{ col }})) in ('reopen','re-open','re_open') then 'reopen'
  else 'unknown'
end
{% endmacro %}

{% macro normalize_txn_type(col) %}
case
  when {{ col }} is null then null
  when lower(trim({{ col }})) in ('payment') then 'payment'
  when lower(trim({{ col }})) in ('reserve') then 'reserve'
  when lower(trim({{ col }})) in ('recovery') then 'recovery'
  else 'unknown'
end
{% endmacro %}

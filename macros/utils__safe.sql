{% macro safe_to_number(col, scale=2) %}
-- Snowflake: try_to_decimal returns null instead of error
try_to_decimal({{ col }}, 38, {{ scale }})
{% endmacro %}

{% macro safe_divide(numer, denom) %}
case
  when {{ denom }} is null or {{ denom }} = 0 then null
  else {{ numer }} / {{ denom }}
end
{% endmacro %}

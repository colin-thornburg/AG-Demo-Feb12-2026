{% test accepted_values_or_unknown(model, column_name, values) %}
-- Ensures normalized enums are within allowed set OR 'unknown' OR null
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not in (
    {%- for v in values -%}
      '{{ v }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
    , 'unknown'
  )
{% endtest %}

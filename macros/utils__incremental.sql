{% macro incremental_where(updated_at_col) %}
-- Use in WHERE clauses: works for full-refresh and incremental.
{% if is_incremental() %}
  where {{ updated_at_col }} >= (
    select coalesce(max({{ updated_at_col }}), '1900-01-01'::timestamp_ntz) from {{ this }}
  )
{% endif %}
{% endmacro %}

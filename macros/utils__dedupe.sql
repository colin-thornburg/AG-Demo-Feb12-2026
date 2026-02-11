{% macro dedupe_latest(relation, key_cols, updated_at_col) %}
-- Returns a SELECT of the latest row per key based on updated_at_col.
-- key_cols: list of column names (strings), e.g. ['policy_id']
with src as (
    select * from {{ relation }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by
            {%- for c in key_cols -%}
              {{ c }}{% if not loop.last %}, {% endif %}
            {%- endfor -%}
            order by {{ updated_at_col }} desc nulls last
        ) as _dbt_rownum
    from src
)
select * exclude (_dbt_rownum)
from ranked
where _dbt_rownum = 1
{% endmacro %}

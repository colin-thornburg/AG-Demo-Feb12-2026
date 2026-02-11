{% macro dedupe_latest(relation, key_cols, updated_at_col) %}
with src as (
    select * from {{ relation }}
),
ranked as (
    select
        *,
        row_number() over (
            partition by
            {% for c in key_cols %}
              {{ c }}{% if not loop.last %}, {% endif %}
            {% endfor %}
            order by {{ updated_at_col }} desc nulls last
        ) as _dbt_rownum
    from src
)
select * exclude (_dbt_rownum)
from ranked
where _dbt_rownum = 1
{% endmacro %}

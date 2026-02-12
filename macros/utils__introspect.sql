{% macro describe_source_columns(source_name, table_name) %}
  {# Returns a YAML-like list of columns/types for a dbt source table. #}
  {% set rel = source(source_name, table_name) %}
  {% set cols = adapter.get_columns_in_relation(rel) %}
  {% for c in cols %}
- name: {{ c.name | lower }}
  data_type: {{ c.data_type }}
  {% endfor %}
{% endmacro %}

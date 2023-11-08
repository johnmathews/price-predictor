{% macro list_tables() %}

  {% set results = run_query('SHOW TABLES IN default') %}
  
  {% if execute %}
    {% for table in results %}
      {{ log(table, info=True) }}
    {% endfor %}
  {% endif %}
  
{% endmacro %}

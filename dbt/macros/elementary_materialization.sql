{# Lets Elementary record test results and failed-row counts (required from dbt 1.8). #}
{% materialization test, default %}
{{ return(elementary.materialization_test_default()) }}
{% endmaterialization %}

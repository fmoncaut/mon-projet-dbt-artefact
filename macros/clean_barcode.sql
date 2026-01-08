{% macro clean_barcode(column_name) %}
    CAST({{ column_name }} AS STRING)
{% endmacro %}
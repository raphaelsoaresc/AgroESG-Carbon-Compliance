{% macro sanitize_encoding(column) %}
    -- Se o byte for inválido para UTF-8, a função retorna NULL e o COALESCE assume 'N/A'
    COALESCE(SAFE_CONVERT_BYTES_TO_STRING(CAST({{ column }} AS BYTES)), 'N/A')
{% endmacro %}
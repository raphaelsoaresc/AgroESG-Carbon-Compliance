{{ config(
    materialized='incremental',
    unique_key='property_id',
    cluster_by=['property_id']
) }}

WITH current_metrics AS (
    -- Pega os dados da staging de topografia (SRTM)
    SELECT * FROM {{ ref('stg_property_topography') }}
)

{% if is_incremental() %}
, historical_metrics AS (
    -- Pega o que já estava salvo nesta mesma tabela (memória histórica)
    SELECT * FROM {{ this }}
)
{% endif %}

SELECT
    curr.property_id,
    curr.grid_id,
    -- LÓGICA DA MEMÓRIA: Mantendo apenas Relevo (Slope e Classificação)
    {% if is_incremental() %}
        COALESCE(curr.max_slope_degrees, hist.max_slope_degrees) as max_slope_degrees,
        COALESCE(curr.relief_classification, hist.relief_classification) as relief_classification,
    {% else %}
        curr.max_slope_degrees,
        curr.relief_classification,
    {% endif %}
    -- Ajustado de last_update para processed_at (nome que está na stg)
    curr.processed_at as last_update
FROM current_metrics curr
{% if is_incremental() %}
LEFT JOIN historical_metrics hist ON curr.property_id = hist.property_id
{% endif %}
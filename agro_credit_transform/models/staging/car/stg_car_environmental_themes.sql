{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'car_temas_ambientais') }}
),

-- Normaliza os números (troca vírgula por ponto)
cleaned_source AS (
    SELECT
        registro_car as property_id,
        file_hash,
        ingested_at,
        
        SAFE_CAST(REPLACE(REPLACE(area_reserva_legal_proposta, '.', ''), ',', '.') AS FLOAT64) as rl_prop,
        SAFE_CAST(REPLACE(REPLACE(area_reserva_legal_averbada, '.', ''), ',', '.') AS FLOAT64) as rl_averb,
        SAFE_CAST(REPLACE(REPLACE(area_preservacao_permanente, '.', ''), ',', '.') AS FLOAT64) as app,
        SAFE_CAST(REPLACE(REPLACE(area_remanescente_vegetacao_nativa, '.', ''), ',', '.') AS FLOAT64) as native_veg,
        SAFE_CAST(REPLACE(REPLACE(area_rural_consolidada, '.', ''), ',', '.') AS FLOAT64) as consolidated,
        SAFE_CAST(REPLACE(REPLACE(area_uso_restrito, '.', ''), ',', '.') AS FLOAT64) as restricted
    FROM source_data
),

unpivoted AS (
    SELECT property_id, 'RESERVA_LEGAL_PROPOSTA' as theme_name, rl_prop as theme_area_ha, file_hash, ingested_at FROM cleaned_source
    UNION ALL
    SELECT property_id, 'RESERVA_LEGAL_AVERBADA', rl_averb, file_hash, ingested_at FROM cleaned_source
    UNION ALL
    SELECT property_id, 'APP', app, file_hash, ingested_at FROM cleaned_source
    UNION ALL
    SELECT property_id, 'VEGETACAO_NATIVA', native_veg, file_hash, ingested_at FROM cleaned_source
    UNION ALL
    SELECT property_id, 'AREA_CONSOLIDADA', consolidated, file_hash, ingested_at FROM cleaned_source
    UNION ALL
    SELECT property_id, 'USO_RESTRITO', restricted, file_hash, ingested_at FROM cleaned_source
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY property_id, theme_name 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM unpivoted
    WHERE theme_area_ha IS NOT NULL AND theme_area_ha > 0
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
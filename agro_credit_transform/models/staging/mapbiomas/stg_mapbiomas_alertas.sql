{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['mapbiomas', 'alerts']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'mapbiomas_alertas_shapes') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identificadores
        CAST(alertid AS INT64) as alert_id,
        CAST(alertcode AS INT64) as alert_code,
        
        -- Datas
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(CAST(detectat AS STRING), 10)),
            SAFE.PARSE_DATE('%d-%m-%Y', LEFT(CAST(detectat AS STRING), 10)),
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(CAST(detectat AS STRING), 10))
        ) as detection_date,
        
        CAST(detectyear AS INT64) as detection_year,
        
        -- Métricas
        SAFE_CAST(alertha AS FLOAT64) as alert_area_ha,
        source as source_satellite,
        biome as biome,
        
        -- Geometria (A CORREÇÃO MÁGICA ESTÁ AQUI)
        -- make_valid => TRUE conserta laços e buracos inválidos
        ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        
        -- Auditoria
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY alert_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
  AND detection_date IS NOT NULL
  AND geometry IS NOT NULL -- Remove geometrias que não puderam ser consertadas
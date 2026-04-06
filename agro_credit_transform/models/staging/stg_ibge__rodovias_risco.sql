{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_rodovias_risco') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Identificador
        CAST(id_origem AS STRING) as road_id,
        
        -- Atributos
        CAST(categoria AS STRING) as category,
        -- Gerando valor fixo 0 já que a coluna não existe na raw
        0 as feature_order, 
        CAST(buffer_m AS INT64) as buffer_meters,
        
        -- Geometria WKT
        CAST(wkt_geom_app AS STRING) as geometry_wkt,
        
        -- Auditoria
        CAST(file_hash AS STRING) as file_hash,
        CAST(ingested_at AS TIMESTAMP) as ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY road_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_cleaned
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
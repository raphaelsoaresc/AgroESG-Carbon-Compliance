{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    -- Referência à tabela populada pela DAG para o IBGE BC250
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_app_zones') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Identificador gerado via row_number() no DuckDB
        CAST(cobacia AS STRING) as water_body_id,
        
        -- Atributos (Para lagos, definimos river_order como 0 na ingestão)
        CAST(river_order AS INT64) as river_order,
        CAST(app_width_m AS FLOAT64) as app_width_meters,
        
        -- Geometria (WKT gerado pelo DuckDB com buffer de 30m)
        wkt_geom_app as geometry_wkt,
        
        -- Auditoria
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        -- Deduplicação baseada no ID gerado
        ROW_NUMBER() OVER (
            PARTITION BY water_body_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_cleaned
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
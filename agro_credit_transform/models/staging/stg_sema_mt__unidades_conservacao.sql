{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'sema_unidades_conservacao') }}
    WHERE OBJECTID IS NOT NULL
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO
        CAST(OBJECTID AS STRING) as uc_id,
        CAST(CODIGO_UC AS STRING) as cnuc_id, -- Código nacional (se houver)
        TRIM(UPPER(CAST(NOME AS STRING))) as uc_name,
        CAST(CATEGORIA AS STRING) as category,
        CAST(GRUPO AS STRING) as group_name,
        'ESTADUAL' as administration_sphere,
        CAST(NULL AS STRING) as biome,
        
        -- DADOS TÉCNICOS
        CAST(ANO_CRIACA AS STRING) as creation_year,
        SAFE_CAST(REPLACE(CAST(AREA_CALCU AS STRING), ',', '.') AS FLOAT64) as area_ha,

        -- GEOMETRIA
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY uc_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num) 
FROM deduplicated 
WHERE row_num = 1
  AND geometry IS NOT NULL
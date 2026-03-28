{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'intermat_assentamentos') }}
    WHERE OBJECTID IS NOT NULL
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO
        CAST(OBJECTID AS STRING) as settlement_id,
        CAST(NUM_SIPRA AS STRING) as sipra_id, -- Pode ser nulo se for 100% estadual
        TRIM(UPPER(CAST(NOME AS STRING))) as settlement_name,
        
        -- LOCALIZAÇÃO
        'MT' as state,
        TRIM(UPPER(CAST(MUNICIPIO AS STRING))) as city,
        
        -- DADOS TÉCNICOS
        CAST(SITUACAO AS STRING) as phase,
        SAFE_CAST(FAMILIAS_B AS INT64) as capacity_families,
        SAFE_CAST(AREA_HA AS FLOAT64) as area_ha,

        -- GEOMETRIA
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY settlement_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num) 
FROM deduplicated 
WHERE row_num = 1
  AND geometry IS NOT NULL
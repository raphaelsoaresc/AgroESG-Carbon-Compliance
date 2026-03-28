{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'incra_assentamentos') }}
    WHERE CD_SIPRA IS NOT NULL -- Código SIPRA é o ID oficial do INCRA
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO
        CAST(CD_SIPRA AS STRING) as settlement_id,
        TRIM(UPPER(CAST(NOME_PROJE AS STRING))) as settlement_name,
        
        -- LOCALIZAÇÃO
        TRIM(UPPER(CAST(UF AS STRING))) as state,
        TRIM(UPPER(CAST(MUNICIPIO AS STRING))) as city,
        
        -- DADOS TÉCNICOS
        CAST(FASE AS STRING) as phase,
        SAFE_CAST(CAPACIDADE AS INT64) as capacity_families,
        SAFE_CAST(AREA_CALC_ AS FLOAT64) as area_ha,

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
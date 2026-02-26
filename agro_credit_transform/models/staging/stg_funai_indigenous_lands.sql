{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'funai_terras_indigenas') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Identificadores
        CAST(terrai_cod AS STRING) as territory_code,
        
        -- Atributos
        UPPER(TRIM(terrai_nom)) as territory_name,
        UPPER(TRIM(etnia_nome)) as ethnicity_name,
        UPPER(TRIM(fase_ti)) as stage_name,
        UPPER(TRIM(modalidade)) as modality_name,
        
        -- Localização
        UPPER(TRIM(uf_sigla)) as state_sigla,
        municipio_ as city_name,
        
        -- Geometria
        geom as geometry_wkt,
        
        -- Auditoria
        file_hash,
        source_filename,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY territory_code 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_cleaned
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
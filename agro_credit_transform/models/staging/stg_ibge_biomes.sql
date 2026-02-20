{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    -- Certifique-se que 'agro_esg_raw' e 'ibge_biomes' estão no seu sources.yml
    SELECT * FROM {{ source('raw_data', 'ibge_biomes') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Identificadores
        -- O IBGE às vezes manda como número, garantimos que seja string
        CAST(CD_Bioma AS STRING) as biome_code, 
        
        -- Atributos
        -- Padronizamos para MAIÚSCULO para facilitar as regras do Código Florestal depois
        -- Ex: 'Amazônia' vira 'AMAZÔNIA'
        UPPER(TRIM(Bioma)) as biome_name,
        
        -- Geometria (WKT vindo do DuckDB/Script Python)
        wkt_geometry as geometry_wkt,
        
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
            PARTITION BY biome_code 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_cleaned
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
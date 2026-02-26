{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'car_sobreposicao') }}
),

renamed_and_filtered AS (
    SELECT
        registro_car as property_id,
        
        -- Descrição geralmente contém o nome da TI/UC ou ID do imóvel sobreposto
        descricao as overlap_name, 
        
        -- Origem indica o tipo (ex: FUNAI, INCRA, ESTADUAL)
        origem as overlap_source,
        
        -- Percentual (Tratamento numérico)
        SAFE_CAST(
            REPLACE(REPLACE(CAST(percentual AS STRING), '.', ''), ',', '.') 
            AS FLOAT64
        ) as overlap_percentage,
        
        -- Área de conflito (Tratamento numérico)
        SAFE_CAST(
            REPLACE(REPLACE(CAST(area_de_conflito AS STRING), '.', ''), ',', '.') 
            AS FLOAT64
        ) as overlap_area_ha,
        
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        -- Deduplicação
        ROW_NUMBER() OVER (
            PARTITION BY property_id, overlap_name, overlap_source
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
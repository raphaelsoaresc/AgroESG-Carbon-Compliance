{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibama_history') }}
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        UF as state,
        MUNICIPIO as city,
        TIPO_AREA as area_type,
        DES_STATUS_FORMULARIO as form_status,
        
        -- Datas
        SAFE_CAST(DAT_EMBARGO AS DATE) as embargo_date,
        SAFE_CAST(DAT_DESEMBARGO AS DATE) as release_date,
        SAFE_CAST(DAT_ULT_ALTER_GEOM AS DATE) as geom_last_modified_date,
        
        -- Lógica de Status Ativo
        CASE 
            WHEN DAT_DESEMBARGO IS NULL THEN TRUE 
            ELSE FALSE 
        END as is_active_embargo,

        -- Geometria e Medidas
        SAFE_CAST(QTD_AREA_EMBARGADA AS FLOAT64) as reported_area_ha,
        NUM_LONGITUDE_TAD as longitude,
        NUM_LATITUDE_TAD as latitude,
        GEOM_AREA_EMBARGADA as geometry_wkt

    FROM source_data
    WHERE UF = 'MT'
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                longitude, 
                latitude, 
                embargo_date, 
                CAST(reported_area_ha AS NUMERIC), -- Convertido para NUMERIC para aceitar no Partition
                form_status
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
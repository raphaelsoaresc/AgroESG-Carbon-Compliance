{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['mapbiomas', 'crossings']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'mapbiomas_alertas_car_sigef') }}
),

properties AS (
    -- Trazemos os IDs das propriedades que monitoramos para filtrar
    -- Importante: Garantir que aqui também esteja limpo (UPPER/TRIM)
    SELECT DISTINCT UPPER(TRIM(property_id)) as property_id 
    FROM {{ ref('stg_car_properties') }}
),

renamed_and_filtered AS (
    SELECT
        CAST(alertid AS INT64) as alert_id,
        
        -- CORREÇÃO: Mantemos os traços (-) pois o seu stg_car_properties também os usa.
        -- Apenas garantimos maiúsculas e sem espaços nas pontas.
        UPPER(TRIM(CAST(codsicar AS STRING))) as car_code,
        
        SAFE_CAST(interha AS FLOAT64) as overlap_area_ha,
        
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY alert_id, car_code 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT 
    d.* EXCEPT(row_num)
FROM deduplicated d
-- AGORA O JOIN VAI FUNCIONAR (MT-123 = MT-123)
INNER JOIN properties p ON d.car_code = p.property_id
WHERE d.row_num = 1
  AND d.overlap_area_ha > 0
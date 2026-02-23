{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'incra_quilombolas') }}
),

renamed_and_cleaned AS (
    SELECT
        -- Identificadores
        CAST(id AS STRING) as territory_code,
        
        -- Atributos
        UPPER(TRIM(name)) as community_name,
        UPPER(TRIM(category_n)) as status_name,
        area_ha,
        
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
{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'incra_quilombolas') }}
    WHERE cd_quilomb IS NOT NULL
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO
        CAST(cd_quilomb AS STRING) as quilombo_id,
        TRIM(UPPER(CAST(nm_comunid AS STRING))) as quilombo_name,
        
        -- LOCALIZAÇÃO
        TRIM(UPPER(CAST(cd_uf AS STRING))) as state,
        TRIM(UPPER(CAST(nm_municip AS STRING))) as city,
        
        -- DADOS TÉCNICOS
        CAST(fase AS STRING) as certification_phase,
        CAST(st_titulad AS STRING) as is_titled,
        SAFE_CAST(nr_familia AS INT64) as families_count,
        SAFE_CAST(nr_area_ha AS FLOAT64) as area_ha,

        -- GEOMETRIA (Convertendo WKT para GEOGRAPHY com correção topológica)
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY quilombo_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num) 
FROM deduplicated 
WHERE row_num = 1
  AND geometry IS NOT NULL
{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'car_area_imovel_geometria') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identifiers
        cod_imovel as property_id,
        
        -- Property Data (Já veio como FLOAT64, não precisa de REPLACE)
        num_area as area_ha,
        mod_fiscal as fiscal_modules,
        
        ind_status as status_code, -- Nome cortado no raw
        des_condic as condition_desc, -- Nome cortado no raw
        ind_tipo as property_type,
        
        -- Location
        municipio as city,
        cod_estado as state,
        
        -- Geometry
        wkt_geom as geometry_wkt,
        
        -- Audit
        file_hash,
        ingested_at,
        source_filename

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY property_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
    AND area_ha > 0
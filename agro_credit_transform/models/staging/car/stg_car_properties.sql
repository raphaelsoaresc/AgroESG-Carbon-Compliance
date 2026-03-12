{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    -- Unindo as 4 tabelas geradas pela DAG
    SELECT * FROM {{ source('raw_data', 'car_area_imovel_geometria_pa') }}
    UNION ALL
    SELECT * FROM {{ source('raw_data', 'car_area_imovel_geometria_mt') }}
    UNION ALL
    SELECT * FROM {{ source('raw_data', 'car_area_imovel_geometria_am') }}
    UNION ALL
    SELECT * FROM {{ source('raw_data', 'car_area_imovel_geometria_ro') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identifiers
        cod_imovel as property_id,
        
        -- Property Data
        num_area as area_ha,
        mod_fiscal as fiscal_modules,
        
        ind_status as status_code, 
        des_condic as condition_desc, 
        ind_tipo as property_type,
        
        -- Location
        municipio as city,
        cod_estado as state,
        uf_origem, -- Nova coluna que injetamos via DuckDB na DAG
        
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
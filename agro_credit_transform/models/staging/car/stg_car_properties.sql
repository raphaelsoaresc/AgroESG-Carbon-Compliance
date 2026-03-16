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
        
        -- Property Data (Com Trava de Sanidade para Hectares vs Metros Quadrados)
        CASE 
            -- Se a área for maior que 500.000 (maior que a maior fazenda do BR), 
            -- assumimos que o produtor digitou em m² e dividimos por 10.000 para converter em ha.
            WHEN SAFE_CAST(num_area AS FLOAT64) > 500000 THEN SAFE_CAST(num_area AS FLOAT64) / 10000
            ELSE SAFE_CAST(num_area AS FLOAT64)
        END as area_ha,

        SAFE_CAST(mod_fiscal AS FLOAT64) as fiscal_modules,
        
        ind_status as status_code, 
        des_condic as condition_desc, 
        ind_tipo as property_type,
        
        -- Location
        municipio as city,
        cod_estado as state,
        uf_origem, 
        
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
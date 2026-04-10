{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
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
        UPPER(TRIM(cod_imovel)) as property_id,
        
        -- LÓGICA DE ÁREA REVISADA
        SAFE_CAST(num_area AS FLOAT64) as area_ha_original,
        CASE 
            WHEN SAFE_CAST(num_area AS FLOAT64) > 10000 THEN SAFE_CAST(num_area AS FLOAT64) / 10000
            ELSE SAFE_CAST(num_area AS FLOAT64)
        END as area_ha_ajustada,
        CASE 
            WHEN SAFE_CAST(num_area AS FLOAT64) <= 0 OR SAFE_CAST(num_area AS FLOAT64) > 1000000 THEN TRUE 
            ELSE FALSE 
        END as is_area_inconsistent,

        SAFE_CAST(mod_fiscal AS FLOAT64) as fiscal_modules,
        
        -- PADRONIZAÇÃO DE STATUS
        CASE 
            WHEN UPPER(TRIM(ind_status)) IN ('AT', 'ATIVO') THEN 'ATIVO'
            WHEN UPPER(TRIM(ind_status)) IN ('PE', 'PENDENTE') THEN 'PENDENTE'
            WHEN UPPER(TRIM(ind_status)) IN ('SU', 'SUSPENSO') THEN 'SUSPENSO'
            WHEN UPPER(TRIM(ind_status)) IN ('CA', 'CANCELADO') THEN 'CANCELADO'
            ELSE UPPER(TRIM(ind_status))
        END as status_code,

        des_condic as condition_desc,

        -- PADRONIZAÇÃO DE TIPO
        CASE 
            WHEN UPPER(TRIM(ind_tipo)) IN ('IRU', 'IMÓVEL RURAL') THEN 'IRU'
            WHEN UPPER(TRIM(ind_tipo)) IN ('AST', 'ASSENTAMENTO') THEN 'AST'
            WHEN UPPER(TRIM(ind_tipo)) IN ('PCT', 'POVOS TRADICIONAIS') THEN 'PCT'
            WHEN UPPER(TRIM(ind_tipo)) IN ('TI', 'TERRA INDÍGENA') THEN 'TI'
            ELSE UPPER(TRIM(ind_tipo))
        END as property_type,
        
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
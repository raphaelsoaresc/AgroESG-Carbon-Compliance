{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['restriction_type', 'priority_level']
) }}

WITH biomes AS (
    SELECT 
        biome_code as restriction_id,
        biome_name as restriction_name,
        'BIOME' as restriction_type,
        biome_name as restriction_subtype,
        CASE 
            WHEN biome_name = 'AMAZÔNIA' THEN 0.80
            WHEN biome_name = 'CERRADO' THEN 0.35
            ELSE 0.20
        END as legal_reserve_perc,
        FALSE as is_hard_block,
        2 as priority_level,
        file_hash,
        source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_ibge_biomes') }}
),

indigenous AS (
    SELECT 
        territory_code as restriction_id,
        territory_name as restriction_name,
        'INDIGENOUS_LAND' as restriction_type,
        stage_name as restriction_subtype,
        NULL as legal_reserve_perc,
        TRUE as is_hard_block,
        1 as priority_level,
        file_hash,
        source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_funai_indigenous_lands') }}
),

quilombolas AS (
    SELECT 
        territory_code as restriction_id,
        community_name as restriction_name,
        'QUILOMBOLA' as restriction_type,
        status_name as restriction_subtype,
        NULL as legal_reserve_perc,
        TRUE as is_hard_block,
        1 as priority_level,
        file_hash,
        source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_incra_quilombola_lands') }}
),

-- NOVA FONTE: APPs de Rios (ANA)
app_rivers AS (
    SELECT 
        basin_code as restriction_id,
        CONCAT('APP RIO - ORDEM ', CAST(river_order AS STRING)) as restriction_name,
        'APP_ZONE' as restriction_type,
        'RIVER' as restriction_subtype,
        NULL as legal_reserve_perc,
        TRUE as is_hard_block,
        1 as priority_level,
        file_hash,
        'ANA_BHO_NIVEL_07' as source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_ana_app_zones') }}
),

-- NOVA FONTE: APPs de Lagos/Represas (IBGE BC250)
app_lakes AS (
    SELECT 
        water_body_id as restriction_id,
        'APP MASSA DAGUA' as restriction_name,
        'APP_ZONE' as restriction_type,
        'WATER_BODY' as restriction_subtype,
        NULL as legal_reserve_perc,
        TRUE as is_hard_block,
        1 as priority_level,
        file_hash,
        'IBGE_BC250_2025' as source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_ibge_bc250_app_zones') }}
),

unioned AS (
    SELECT * FROM biomes
    UNION ALL
    SELECT * FROM indigenous
    UNION ALL
    SELECT * FROM quilombolas
    UNION ALL
    SELECT * FROM app_rivers
    UNION ALL
    SELECT * FROM app_lakes
),

spatial_processing AS (
    SELECT
        * EXCEPT(geometry_wkt),
        -- Converte o WKT para GEOGRAPHY. 
        -- O DuckDB já simplificou, então aqui o processamento será rápido.
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry_raw
    FROM unioned
),

final_enriched AS (
    SELECT
        * EXCEPT(geometry_raw),
        -- Mantemos a geometria e calculamos a BBox para o matching de alta performance
        geometry_raw as geometry,
        ST_BOUNDINGBOX(geometry_raw) as bbox
    FROM spatial_processing
    WHERE geometry_raw IS NOT NULL
)

SELECT * FROM final_enriched
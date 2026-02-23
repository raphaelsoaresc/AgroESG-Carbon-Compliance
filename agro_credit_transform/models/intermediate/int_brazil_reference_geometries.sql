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
        2 as priority_level, -- Prioridade menor (regra de % de reserva)
        -- Linhagem vinda da DAG/STG
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
        1 as priority_level, -- Prioridade máxima (bloqueio total)
        file_hash,
        source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_funai_terras_indigenas') }}
),

quilombolas AS (
    SELECT 
        territory_code as restriction_id,
        community_name as restriction_name,
        'QUILOMBOLA' as restriction_type,
        status_name as restriction_subtype,
        NULL as legal_reserve_perc,
        TRUE as is_hard_block,
        1 as priority_level, -- Prioridade máxima (bloqueio total)
        file_hash,
        source_filename,
        ingested_at,
        geometry_wkt
    FROM {{ ref('stg_incra_quilombolas') }}
),

unioned AS (
    SELECT * FROM biomes
    UNION ALL
    SELECT * FROM indigenous
    UNION ALL
    SELECT * FROM quilombolas
),

spatial_processing AS (
    SELECT
        *,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry_raw
    FROM unioned
),

final_enriched AS (
    SELECT
        * EXCEPT(geometry_wkt, geometry_raw),
        ST_SIMPLIFY(geometry_raw, 100) as geometry,
        ST_BOUNDINGBOX(geometry_raw) as bbox
    FROM spatial_processing
    WHERE geometry_raw IS NOT NULL
)

SELECT * FROM final_enriched
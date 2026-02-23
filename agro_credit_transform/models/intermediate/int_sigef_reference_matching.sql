{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id', 'priority_level']
) }}

WITH property_base AS (
    SELECT
        property_id,
        ST_CENTROID(geom_calc) as centroid,
        geom_calc as full_geom
    FROM {{ ref('int_sigef_geometries') }}
),

-- 1. MATCHING DE BIOMAS (Com tolerância de 1km para evitar "Gaps")
biomes_match AS (
    SELECT
        p.property_id,
        ref.restriction_id,
        ref.restriction_name,
        ref.restriction_type,
        ref.restriction_subtype,
        ref.legal_reserve_perc,
        ref.is_hard_block,
        ref.priority_level,
        ref.file_hash as ref_file_hash,
        ref.source_filename as ref_source_filename,
        ref.ingested_at as ref_ingested_at,
        -- A distância servirá para o QUALIFY pegar o bioma mais próximo/correto
        ST_DISTANCE(p.centroid, ref.geometry) as dist_score
    FROM property_base p
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} ref
        ON ref.restriction_type = 'BIOME'
        -- Tolerância de 1000 metros resolve os 8 casos "Desconhecidos"
        AND ST_DWITHIN(p.centroid, ref.geometry, 1000)
),

-- 2. MATCHING DE HARD BLOCKS (Mantido via Polígono para segurança total)
hard_blocks_match AS (
    SELECT
        p.property_id,
        ref.restriction_id,
        ref.restriction_name,
        ref.restriction_type,
        ref.restriction_subtype,
        ref.legal_reserve_perc,
        ref.is_hard_block,
        ref.priority_level,
        ref.file_hash as ref_file_hash,
        ref.source_filename as ref_source_filename,
        ref.ingested_at as ref_ingested_at,
        0 as dist_score
    FROM property_base p
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} ref
        ON ref.restriction_type != 'BIOME'
        AND ST_INTERSECTSBOX(p.full_geom, ref.bbox.xmin, ref.bbox.ymin, ref.bbox.xmax, ref.bbox.ymax)
        AND ST_INTERSECTS(p.full_geom, ref.geometry)
),

unioned_matches AS (
    SELECT * FROM biomes_match
    UNION ALL
    SELECT * FROM hard_blocks_match
)

SELECT
    * EXCEPT(dist_score)
FROM unioned_matches
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY property_id, restriction_type 
    ORDER BY dist_score ASC
) = 1
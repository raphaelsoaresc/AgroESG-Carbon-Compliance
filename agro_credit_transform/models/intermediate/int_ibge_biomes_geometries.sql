{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='biome_name'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_ibge_biomes') }}
),

spatial_processing AS (
    SELECT
        biome_code,
        biome_name,
        file_hash,
        ingested_at,
        -- 1. Converte e valida
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry
    FROM staging_data
),

enriched_data AS (
    SELECT
        biome_code,
        biome_name,
        file_hash,
        ingested_at,
        
        ST_BOUNDINGBOX(geometry) as bbox,
        geometry,

        -- Regra de Negócio: Reserva Legal
        CASE 
            WHEN biome_name = 'AMAZÔNIA' THEN 0.80
            WHEN biome_name = 'CERRADO' THEN 0.35
            ELSE 0.20
        END as legal_reserve_perc
    FROM spatial_processing
    WHERE geometry IS NOT NULL
)

SELECT * FROM enriched_data

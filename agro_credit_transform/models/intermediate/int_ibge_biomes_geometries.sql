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
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry_raw
    FROM staging_data
),

enriched_data AS (
    SELECT
        biome_code,
        biome_name,
        file_hash,
        ingested_at,
        
        -- 2. SIMPLIFICAÇÃO: Biomas não precisam de precisão de cm. 
        -- 100 metros de tolerância reduz drasticamente o número de vértices.
        ST_SIMPLIFY(geometry_raw, 100) as geometry,

        -- 3. BOUNDING BOX: Útil para filtros ST_INTERSECTSBOX ultra rápidos no Mart
        -- O BigQuery agradece se você já deixar os limites calculados.
        ST_BOUNDINGBOX(geometry_raw) as bbox,

        -- Regra de Negócio: Reserva Legal
        CASE 
            WHEN biome_name = 'AMAZÔNIA' THEN 0.80
            WHEN biome_name = 'CERRADO' THEN 0.35
            ELSE 0.20
        END as legal_reserve_perc
    FROM spatial_processing
    WHERE geometry_raw IS NOT NULL
)

SELECT * FROM enriched_data

{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id']
) }}

WITH property_centroids AS (
    SELECT
        property_id,
        ST_CENTROID(geom_calc) as centroid -- Gera um ponto único
    FROM {{ ref('int_sigef_geometries') }}
)

SELECT
    pc.property_id,
    b.biome_name,
    b.legal_reserve_perc
FROM property_centroids pc
INNER JOIN {{ ref('int_ibge_biomes_geometries') }} b 
    ON ST_INTERSECTS(pc.centroid, b.geometry) -- Cruzamento PONTO-POLÍGONO (muito leve)

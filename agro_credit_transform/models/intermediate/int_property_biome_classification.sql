{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id']
) }}

WITH property_centroids AS (
    SELECT
        property_id,
        ST_CENTROID(geom_calc) as centroid
    FROM {{ ref('int_sigef_geometries') }}
),

-- Cruzamento com tolerância (Snap)
matched_biomes AS (
    SELECT
        pc.property_id,
        b.biome_name,
        b.legal_reserve_perc,
        -- Calculamos a distância para desempatar caso o ponto esteja perto de 2 biomes
        ST_DISTANCE(pc.centroid, b.geometry) as distance_to_biome
    FROM property_centroids pc
    INNER JOIN {{ ref('int_ibge_biomes_geometries') }} b 

        ON ST_DWITHIN(pc.centroid, b.geometry, 500) 
)

SELECT
    property_id,
    biome_name,
    legal_reserve_perc
FROM matched_biomes
-- Se o ponto estiver perto de 2 biomas (ex: fronteira Cerrado/Amazônia),
-- pegamos aquele que estiver matematicamente mais perto (menor distância).
QUALIFY ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY distance_to_biome ASC) = 1
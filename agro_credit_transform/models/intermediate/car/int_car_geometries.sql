{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry',
    tags=['car']
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_car_properties') }}
),

spatial_processing AS (
    SELECT
        * EXCEPT(geometry_wkt),
        -- REMOVIDO O REGEX COMPLEXO.
        -- Usamos apenas SAFE.ST_GEOGFROMTEXT.
        -- O make_valid => TRUE corrige polígonos com auto-interseção (laços).
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry
    FROM staging_data
    WHERE geometry_wkt IS NOT NULL
),

final_cleaning AS (
    SELECT 
        * EXCEPT(geometry),
        geometry,
        -- Centróide para joins rápidos
        ST_CENTROID(geometry) as centroid,
        -- Simplificação leve (20m) para reduzir tamanho em bytes no PowerBI/Mapas
        ST_SIMPLIFY(geometry, 20) as geom_simplified,
        -- Deduplicação final por ID
        ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) as rn
    FROM spatial_processing
    -- Filtra geometrias que falharam na conversão (NULL)
    WHERE geometry IS NOT NULL
        -- Garante que só passamos Polígonos (remove pontos ou linhas perdidas)
        AND ST_GEOMETRYTYPE(geometry) IN ('ST_Polygon', 'ST_MultiPolygon')
)

SELECT * EXCEPT(rn) 
FROM final_cleaning 
WHERE rn = 1
{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry'
) }}

WITH base_cleaning AS (
    SELECT 
        * EXCEPT(geometry_wkt),
        geometry_wkt as original_wkt,
        REGEXP_REPLACE(geometry_wkt, r'^SRID=\d+;', '') as cleaned_wkt
    FROM {{ ref('stg_ibama') }}
),

spatial_processing AS (
    SELECT
        *,
        SAFE.ST_GEOGFROMTEXT(cleaned_wkt, make_valid => TRUE) as poly_geom,
        -- Validação de coordenadas após a limpeza do staging
        CASE 
            WHEN longitude IS NOT NULL AND latitude IS NOT NULL 
                AND longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90
            THEN SAFE.ST_GEOGPOINT(longitude, latitude)
            ELSE NULL 
        END as point_geom
    FROM base_cleaning
),

final_geometry AS (
    SELECT
        * EXCEPT(poly_geom, point_geom, cleaned_wkt),
        -- Se não tiver polígono, usa o ponto com buffer de ~50m
        COALESCE(poly_geom, ST_BUFFER(point_geom, 0.0005)) as geometry,
        -- Auditoria de origem da geometria
        CASE 
            WHEN poly_geom IS NOT NULL THEN 'POLYGON'
            WHEN point_geom IS NOT NULL THEN 'POINT_BUFFER'
            ELSE 'NO_GEOM'
        END as geom_source
    FROM spatial_processing
)

SELECT * FROM final_geometry 
WHERE geometry IS NOT NULL 
OR original_wkt IS NOT NULL 
OR (longitude IS NOT NULL AND latitude IS NOT NULL)

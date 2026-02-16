{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry'
) }}
WITH staging_data AS (
    SELECT * FROM {{ ref('stg_ibama') }}
),

spatial_processing AS (
    SELECT
        *,
        -- 1. Tenta converter o WKT (Polígono)
        SAFE.ST_GEOGFROMTEXT(geometry_wkt) as poly_geom,
        
        -- 2. Cria o Ponto a partir das coordenadas (Convertendo explicitamente para FLOAT64)
        SAFE.ST_GEOGPOINT(
            SAFE_CAST(longitude AS FLOAT64), 
            SAFE_CAST(latitude AS FLOAT64)
        ) as point_geom
    FROM staging_data
),

final_geometry AS (
    SELECT
        * EXCEPT(poly_geom, point_geom, geometry_wkt),
        
        -- Lógica de Prioridade: Polígono > Ponto com Buffer
        CASE 
            WHEN poly_geom IS NOT NULL THEN poly_geom
            WHEN point_geom IS NOT NULL THEN ST_BUFFER(point_geom, 50)
            ELSE NULL
        END as geometry
    FROM spatial_processing
)

SELECT * FROM final_geometry
WHERE geometry IS NOT NULL
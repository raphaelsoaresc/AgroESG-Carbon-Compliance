{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_sigef') }}
),

spatial_cleaning AS (
    SELECT
        * EXCEPT(geometry_wkt),
        -- Limpeza específica para SIGEF que muitas vezes vem com coordenadas Z (3D)
        REGEXP_REPLACE(
            REPLACE(UPPER(geometry_wkt), ' Z ', ' '),
            r' [0-9.-]+([,)])',
            r'\1'
        ) as clean_wkt
    FROM staging_data
),

spatial_processing AS (
    SELECT
        * EXCEPT(clean_wkt),
        -- O BigQuery já valida aqui com o parâmetro make_valid => TRUE
        SAFE.ST_GEOGFROMTEXT(clean_wkt, make_valid => TRUE) as geometry
    FROM spatial_cleaning
),

final_cleaning AS (
    SELECT 
        * EXCEPT(geometry),
        geometry,
        -- Já entrega a versão simplificada para o Mart não ter trabalho
        ST_SIMPLIFY(geometry, 20) as geom_calc,
        -- Garante unicidade por ID
        ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) as rn
    FROM spatial_processing
    WHERE geometry IS NOT NULL
    -- Filtra apenas Polígonos e MultiPolígonos
        AND ST_GEOMETRYTYPE(geometry) IN ('ST_Polygon', 'ST_MultiPolygon')
)

SELECT * EXCEPT(rn) 
FROM final_cleaning 
WHERE rn = 1
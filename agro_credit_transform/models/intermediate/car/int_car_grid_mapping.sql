{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['grid_id'],
    tags=['car', 'earth_engine', 'grid']
) }}

WITH base_geometries AS (
    SELECT 
        property_id,
        centroid
    FROM {{ ref('int_car_geometries') }}
    -- Garantir que não tentamos processar fazendas sem geometria válida
    WHERE centroid IS NOT NULL 
),

extract_coordinates AS (
    SELECT
        property_id,
        -- No BigQuery Spatial: ST_Y é a Latitude, ST_X é a Longitude
        ST_Y(centroid) AS original_latitude,
        ST_X(centroid) AS original_longitude
    FROM base_geometries
),

round_coordinates AS (
    SELECT
        property_id,
        original_latitude,
        original_longitude,
        -- Arredondamento para 1 casa decimal (aprox. 11 km de resolução)
        ROUND(original_latitude, 1) AS latitude_rounded,
        ROUND(original_longitude, 1) AS longitude_rounded
    FROM extract_coordinates
),

generate_grid AS (
    SELECT
        property_id,
        original_latitude,
        original_longitude,
        latitude_rounded,
        longitude_rounded,
        -- Criação da String do ID. Exemplo: 'GRID_-12.5_-55.8'
        CONCAT('GRID_', CAST(latitude_rounded AS STRING), '_', CAST(longitude_rounded AS STRING)) AS grid_id
    FROM round_coordinates
)

SELECT 
    property_id,
    grid_id,
    latitude_rounded,
    longitude_rounded,
    original_latitude,
    original_longitude,
    CURRENT_TIMESTAMP() AS mapped_at
FROM generate_grid
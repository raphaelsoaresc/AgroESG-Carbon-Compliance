-- models/intermediate/car/int_car_self_overlap.sql
{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['grid_id'],
    tags=['car', 'compliance', 'spatial']
) }}

{% set gis_noise_ha = var('gis_noise_ha_threshold', 0.1) %}

WITH car_data AS (
    -- Unimos a geometria com o grid para o join performático
    SELECT 
        g.property_id,
        g.geometry,
        g.area_ha,
        grid.grid_id
    FROM {{ ref('int_car_geometries') }} g
    INNER JOIN {{ ref('int_car_grid_mapping') }} grid ON g.property_id = grid.property_id
),

spatial_intersection AS (
    -- O segredo da performance: Join pelo grid_id primeiro
    SELECT 
        a.property_id,
        a.grid_id, -- Mantemos o grid_id para o cluster_by final
        b.property_id as overlapping_property_id,
        a.area_ha as original_area_ha,
        -- Calculamos a área da intersecção em hectares
        ST_AREA(ST_INTERSECTION(a.geometry, b.geometry)) / 10000 as overlap_area_ha
    FROM car_data a
    INNER JOIN car_data b ON a.grid_id = b.grid_id -- Filtro geográfico bruto
    WHERE a.property_id != b.property_id -- Não comparar a fazenda com ela mesma
      AND ST_INTERSECTS(a.geometry, b.geometry) -- Filtro geográfico preciso
),

aggregated_overlaps AS (
    SELECT 
        property_id,
        grid_id,
        MAX(original_area_ha) as area_ha, -- Carregamos a área para evitar subquery no select final
        COUNT(DISTINCT overlapping_property_id) as total_overlapping_cars,
        SUM(overlap_area_ha) as total_overlap_ha
    FROM spatial_intersection
    -- Filtramos ruídos (ex: sobreposições milimétricas de borda)
    WHERE overlap_area_ha > {{ gis_noise_ha }}
    GROUP BY 1, 2
)

SELECT 
    property_id,
    grid_id, -- OBRIGATÓRIO estar aqui para o cluster_by funcionar
    total_overlapping_cars,
    total_overlap_ha,
    -- Percentual de sobreposição em relação à área total da fazenda
    SAFE_DIVIDE(total_overlap_ha, area_ha) as overlap_pct,
    CURRENT_TIMESTAMP() as calculated_at
FROM aggregated_overlaps
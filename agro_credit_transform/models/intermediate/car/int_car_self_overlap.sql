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
        g.geometry_raw,
        g.geometry_simplified,
        -- Otimização 1: Cálculo da Bounding Box (retângulo envolvente)
        -- Retorna uma STRUCT com xmin, xmax, ymin, ymax
        ST_BOUNDINGBOX(g.geometry_simplified) as bbox,
        g.area_ha,
        grid.grid_id
    FROM {{ ref('int_car_geometries') }} g
    INNER JOIN {{ ref('int_car_grid_mapping') }} grid ON g.property_id = grid.property_id
    WHERE 1=1
    {% if var('target_uf', none) %}
        -- Filtro de fatiamento: processa apenas um estado por vez para evitar sobrecarga
        AND g.state = '{{ var("target_uf") }}' 
    {% endif %}
),

spatial_intersection AS (
    -- O segredo da performance: Join pelo grid_id + Filtros em cascata
    SELECT 
        a.property_id,
        a.grid_id,
        b.property_id as overlapping_property_id,
        a.area_ha as original_area_ha,
        -- O cálculo pesado (ST_INTERSECTION) só ocorre para quem passar nos filtros do WHERE
        ST_AREA(ST_INTERSECTION(a.geometry_raw, b.geometry_raw)) / 10000 as overlap_area_ha
    FROM car_data a
    INNER JOIN car_data b ON a.grid_id = b.grid_id 
    WHERE a.property_id != b.property_id 
      -- Otimização 2: Comparação matemática de Bounding Boxes
      -- Descarta ~90% dos casos sem custo computacional de funções geográficas
      AND a.bbox.xmin <= b.bbox.xmax 
      AND a.bbox.xmax >= b.bbox.xmin 
      AND a.bbox.ymin <= b.bbox.ymax 
      AND a.bbox.ymax >= b.bbox.ymin
      
      -- Filtro rápido (Geometria Simplificada)
      -- Refina a busca antes do cálculo de área exata usando GEOGRAPHY
      AND ST_INTERSECTS(a.geometry_simplified, b.geometry_simplified) 
),

aggregated_overlaps AS (
    SELECT 
        property_id,
        grid_id,
        MAX(original_area_ha) as area_ha,
        COUNT(DISTINCT overlapping_property_id) as total_overlapping_cars,
        SUM(overlap_area_ha) as total_overlap_ha
    FROM spatial_intersection
    -- Filtramos ruídos (ex: sobreposições milimétricas de borda/precisão digital)
    WHERE overlap_area_ha > {{ gis_noise_ha }}
    GROUP BY 1, 2
)

SELECT 
    property_id,
    grid_id,
    total_overlapping_cars,
    total_overlap_ha,
    -- Percentual de sobreposição em relação à área total da fazenda
    SAFE_DIVIDE(total_overlap_ha, area_ha) as overlap_pct,
    CURRENT_TIMESTAMP() as calculated_at
FROM aggregated_overlaps
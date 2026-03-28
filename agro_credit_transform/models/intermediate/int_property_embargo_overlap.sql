{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='property_id'
) }}

WITH intersections AS (
    SELECT
        c.property_id,
        e.source as embargo_source,
        e.embargo_date,
        e.is_cancelled,
        e.is_active_embargo,
        e.offender_name,
        e.process_number,
        e.reported_area_ha,
        e.tax_id,
        -- Em vez de calcular a área aqui, guardamos a GEOMETRIA da intersecção
        ST_INTERSECTION(c.geometry_raw, e.geometry) as intersected_geom
    FROM {{ ref('int_car_geometries') }} c
    INNER JOIN {{ ref('int_all_embargoes') }} e 
        ON ST_INTERSECTS(c.geometry_raw, e.geometry)
)

SELECT
    property_id,
    MIN(embargo_date) as earliest_embargo_date,
    
    -- A MÁGICA ACONTECE AQUI: 
    -- ST_UNION_AGG funde todos os embargos sobrepostos em um só.
    -- Depois ST_AREA calcula a área real sem dupla contagem.
    ST_AREA(ST_UNION_AGG(intersected_geom)) / 10000 as total_embargo_area_ha,
    
    -- Usamos COALESCE para garantir que o Mart receba FALSE e não NULL
    COALESCE(LOGICAL_OR(is_active_embargo), FALSE) as has_any_active_embargo,
    COALESCE(LOGICAL_OR(is_cancelled), FALSE) as has_any_cancelled_embargo,
    COUNT(*) as total_embargo_records,
    ARRAY_AGG(DISTINCT embargo_source) as embargo_sources,
    
    -- Novas colunas periciais agregadas para evitar duplicação de linhas
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT offender_name IGNORE NULLS), ' | ') as embargo_offenders,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT process_number IGNORE NULLS), ' | ') as embargo_processes,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT tax_id IGNORE NULLS), ' | ') as embargo_tax_ids,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(reported_area_ha AS STRING) IGNORE NULLS), ' | ') as embargo_reported_areas

FROM intersections
-- Filtra sujeiras espaciais menores que 10 metros quadrados (0.001 ha)
WHERE ST_AREA(intersected_geom) > 10 
GROUP BY 1
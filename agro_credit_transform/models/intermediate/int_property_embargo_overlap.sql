{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='property_id'
) }}

WITH intersections AS (
    SELECT
        s.property_id,
        i.embargo_date,
        -- Cálculo da área de sobreposição convertida para hectares
        ST_AREA(ST_INTERSECTION(s.geom_calc, i.geometry)) / 10000 as overlap_ha
    FROM {{ ref('int_sigef_geometries') }} s
    INNER JOIN {{ ref('int_ibama_geometries') }} i 
        ON ST_INTERSECTS(s.geom_calc, i.geometry)
    -- Filtro espacial rápido (Bounding Box do MT) antes do cálculo pesado
    WHERE ST_INTERSECTSBOX(i.geometry, -61.7, -18.1, -50.1, -7.3)
)

SELECT
    property_id,
    MIN(embargo_date) as earliest_embargo_date,
    SUM(overlap_ha) as total_embargo_area_ha,
    COUNT(*) as total_embargo_records
FROM intersections
GROUP BY 1

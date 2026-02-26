{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='property_id',
    tags=['car']
) }}

WITH properties AS (
    SELECT property_id, geometry 
    FROM {{ ref('int_car_geometries') }}
),

hard_blocks AS (
    SELECT 
        restriction_id,
        restriction_name,
        restriction_type, 
        geometry
    FROM {{ ref('int_brazil_reference_geometries') }}
    WHERE is_hard_block = TRUE
    AND restriction_type IN ('INDIGENOUS_LAND', 'QUILOMBOLA', 'CONSERVATION_UNIT')
),

intersections AS (
    SELECT
        p.property_id,
        hb.restriction_type,
        hb.restriction_name,
        ST_AREA(ST_INTERSECTION(p.geometry, hb.geometry)) / 10000 as overlap_ha
    FROM properties p
    INNER JOIN hard_blocks hb
        ON ST_INTERSECTS(p.geometry, hb.geometry)
    WHERE ST_AREA(ST_INTERSECTION(p.geometry, hb.geometry)) / 10000 > 0.1
)

SELECT
    property_id,
    ARRAY_AGG(STRUCT(restriction_type, restriction_name, overlap_ha)) as overlaps_details,
    SUM(overlap_ha) as total_overlap_ha,
    TRUE as has_hard_block_overlap
FROM intersections
GROUP BY 1
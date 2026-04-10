{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id']
) }}

WITH props AS (
    SELECT property_id, geometry_raw FROM {{ ref('int_car_geometries') }}
    WHERE geometry_raw IS NOT NULL
),

water_features AS (
    SELECT restriction_id, restriction_name, geometry 
    FROM {{ ref('int_brazil_reference_geometries') }}
    WHERE restriction_type = 'WATER_BODY_FEATURE' 
      AND is_man_made = TRUE
)

SELECT 
    p.property_id,
    COUNT(DISTINCT w.restriction_id) as count_artificial_water_bodies,
    STRING_AGG(DISTINCT w.restriction_name, ' | ') as artificial_water_details,
    CURRENT_TIMESTAMP() as processed_at
FROM props p
INNER JOIN water_features w ON ST_INTERSECTS(p.geometry_raw, w.geometry)
GROUP BY 1
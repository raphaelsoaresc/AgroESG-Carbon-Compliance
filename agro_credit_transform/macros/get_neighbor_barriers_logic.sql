{% macro get_neighbor_barriers_logic(state_code) %}

WITH props_with_grid AS (
    SELECT 
        g.property_id,
        g.grid_id,
        geom.centroid,
        geom.geometry as geometry_original,
        ST_SIMPLIFY(geom.geometry, 0.0001) as geometry_smpl
    FROM {{ ref('int_car_grid_mapping') }} g
    INNER JOIN {{ ref('int_car_geometries') }} geom ON g.property_id = geom.property_id
    WHERE geom.state = '{{ state_code }}'
      AND geom.geometry IS NOT NULL
),

prop_to_roads AS (
    SELECT 
        p.property_id,
        r.restriction_id as road_id,
        r.restriction_name as road_name
    FROM props_with_grid p
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} r 
        ON ST_INTERSECTS(p.geometry_original, r.geometry)
    WHERE r.restriction_type = 'INFRASTRUCTURE' 
      AND r.restriction_subtype = 'ROAD'
),

neighbor_pairs AS (
    -- Substituído DISTINCT por GROUP BY para suportar GEOGRAPHY no BigQuery
    SELECT
        p1.property_id,
        p2.property_id as neighbor_id,
        ANY_VALUE(p1.centroid) as p1_centroid,
        ANY_VALUE(p2.centroid) as p2_centroid,
        MIN(ST_DISTANCE(p1.geometry_smpl, p2.geometry_smpl)) as distance_meters
    FROM props_with_grid p1
    INNER JOIN props_with_grid p2 ON p1.grid_id = p2.grid_id
    WHERE p1.property_id < p2.property_id
      AND ST_DWITHIN(p1.geometry_smpl, p2.geometry_smpl, 1000)
    GROUP BY 1, 2
),

road_connectivity AS (
    SELECT 
        np.property_id,
        np.neighbor_id,
        ANY_VALUE(pr1.road_id) as road_id,
        ANY_VALUE(pr1.road_name) as road_name
    FROM neighbor_pairs np
    INNER JOIN prop_to_roads pr1 ON np.property_id = pr1.property_id
    INNER JOIN prop_to_roads pr2 ON np.neighbor_id = pr2.property_id
    WHERE pr1.road_id = pr2.road_id
    GROUP BY 1, 2
),

barrier_analysis AS (
    SELECT 
        np.property_id,
        np.neighbor_id,
        LOGICAL_OR(ST_INTERSECTS(ST_MAKELINE(np.p1_centroid, np.p2_centroid), rb.geometry)) as has_big_river_barrier
    FROM neighbor_pairs np
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} rb 
        ON ST_INTERSECTS(ST_MAKELINE(np.p1_centroid, np.p2_centroid), rb.geometry)
    WHERE rb.restriction_type = 'APP_ZONE' 
      AND rb.restriction_subtype = 'RIVER'
      AND (rb.restriction_name LIKE '%ORDEM 3%' OR rb.restriction_name LIKE '%ORDEM 4%' OR rb.restriction_name LIKE '%ORDEM 5%')
    GROUP BY 1, 2
)

SELECT 
    np.property_id,
    np.neighbor_id,
    '{{ state_code }}' as state,
    np.distance_meters,
    CASE WHEN np.distance_meters = 0 THEN TRUE ELSE FALSE END as shares_border,
    COALESCE(ba.has_big_river_barrier, FALSE) as is_physically_blocked,
    CASE WHEN rc.road_id IS NOT NULL THEN TRUE ELSE FALSE END as has_road_connection,
    rc.road_id as connected_road_id,
    rc.road_name as connected_road_name
FROM neighbor_pairs np
LEFT JOIN barrier_analysis ba 
    ON np.property_id = ba.property_id AND np.neighbor_id = ba.neighbor_id
LEFT JOIN road_connectivity rc 
    ON np.property_id = rc.property_id AND np.neighbor_id = rc.neighbor_id

{% endmacro %}
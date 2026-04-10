{% macro get_neighbor_barriers_logic(state_code) %}

WITH props_base AS (
    SELECT 
        g.property_id,
        g.grid_id,
        geom.geometry_raw as geometry_original
    FROM {{ ref('int_car_grid_mapping') }} g
    INNER JOIN {{ ref('int_car_geometries') }} geom ON g.property_id = geom.property_id
    WHERE geom.state = '{{ state_code }}'
      AND geom.geometry_raw IS NOT NULL
),

-- 1. MAPEAMENTO DE INFRAESTRUTURA POR PROPRIEDADE
prop_to_infra AS (
    SELECT 
        p.property_id,
        r.restriction_id as infra_id,
        r.restriction_name as infra_name,
        r.restriction_subtype as infra_type
    FROM props_base p
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} r 
        ON ST_INTERSECTS(p.geometry_original, r.geometry)
    WHERE r.restriction_type = 'INFRASTRUCTURE'
),

-- 2. IDENTIFICAÇÃO DE VIZINHOS PELAS MARGENS (ST_DISTANCE)
neighbor_pairs AS (
    SELECT
        p1.property_id,
        p2.property_id as neighbor_id,
        ANY_VALUE(p1.geometry_original) as p1_geom,
        ANY_VALUE(p2.geometry_original) as p2_geom,
        ANY_VALUE(ST_CLOSESTPOINT(p1.geometry_original, p2.geometry_original)) as contact_point,
        MIN(ST_DISTANCE(p1.geometry_original, p2.geometry_original)) as distance_meters
    FROM props_base p1
    INNER JOIN props_base p2 ON p1.grid_id = p2.grid_id
    WHERE p1.property_id < p2.property_id -- Mantemos a "mão única" aqui para performance
      AND ST_DWITHIN(p1.geometry_original, p2.geometry_original, 500)
    GROUP BY 1, 2
),

-- 3. CONECTIVIDADE POR INFRAESTRUTURA COMPARTILHADA
shared_connectivity AS (
    SELECT 
        np.property_id,
        np.neighbor_id,
        STRING_AGG(DISTINCT CASE WHEN i1.infra_type = 'ROAD' THEN i1.infra_name END, ' | ') as shared_roads,
        STRING_AGG(DISTINCT CASE WHEN i1.infra_type = 'POWER_LINE' THEN i1.infra_name END, ' | ') as shared_power_lines,
        LOGICAL_OR(i1.infra_type = 'ROAD') as has_road_connection,
        LOGICAL_OR(i1.infra_type = 'POWER_LINE') as has_power_connection
    FROM neighbor_pairs np
    INNER JOIN prop_to_infra i1 ON np.property_id = i1.property_id
    INNER JOIN prop_to_infra i2 ON np.neighbor_id = i2.property_id
    WHERE i1.infra_id = i2.infra_id
    GROUP BY 1, 2
),

-- 4. ANÁLISE DE BARREIRA FÍSICA (RIOS)
barrier_analysis AS (
    SELECT 
        np.property_id,
        np.neighbor_id,
        ANY_VALUE(rb.restriction_name) as river_name,
        LOGICAL_OR(
            ST_INTERSECTS(
                ST_MAKELINE(
                    ST_CLOSESTPOINT(np.p1_geom, np.p2_geom), 
                    ST_CLOSESTPOINT(np.p2_geom, np.p1_geom)
                ), 
                rb.geometry
            )
        ) as has_river_barrier
    FROM neighbor_pairs np
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} rb 
        ON ST_INTERSECTS(
            ST_MAKELINE(
                ST_CLOSESTPOINT(np.p1_geom, np.p2_geom), 
                ST_CLOSESTPOINT(np.p2_geom, np.p1_geom)
            ), 
            rb.geometry
        )
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
    np.contact_point,
    COALESCE(sc.has_road_connection, FALSE) as has_road_connection,
    COALESCE(sc.has_power_connection, FALSE) as has_power_connection,
    CASE 
        WHEN ba.has_river_barrier = TRUE 
             AND COALESCE(sc.has_road_connection, FALSE) = FALSE 
             AND COALESCE(sc.has_power_connection, FALSE) = FALSE 
        THEN TRUE 
        ELSE FALSE 
    END as is_physically_blocked,
    sc.shared_roads as connected_roads_names,
    sc.shared_power_lines as connected_power_lines_names,
    ba.river_name as barrier_river_name,
    CURRENT_TIMESTAMP() as calculated_at
FROM neighbor_pairs np
LEFT JOIN shared_connectivity sc ON np.property_id = sc.property_id AND np.neighbor_id = sc.neighbor_id
LEFT JOIN barrier_analysis ba ON np.property_id = ba.property_id AND np.neighbor_id = ba.neighbor_id

{% endmacro %}
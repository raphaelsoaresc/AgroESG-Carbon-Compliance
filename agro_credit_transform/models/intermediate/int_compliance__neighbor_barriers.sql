{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['state', 'property_id', 'neighbor_id']
) }}

WITH unioned_states AS (
    SELECT * FROM {{ ref('int_neighbor_barriers_mt') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_pa') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_ro') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_am') }}
),

mirrored AS (
    -- Lado original (A -> B)
    SELECT * FROM unioned_states
    
    UNION ALL
    
    -- Lado espelhado (B -> A)
    SELECT 
        neighbor_id as property_id,
        property_id as neighbor_id,
        state,
        distance_meters,
        contact_point,
        has_road_connection,
        has_power_connection,
        is_physically_blocked,
        connected_roads_names,
        connected_power_lines_names,
        barrier_river_name,
        calculated_at
    FROM unioned_states
)

SELECT * FROM mirrored
{{ config(
    materialized='table',
    cluster_by=['state', 'property_id', 'neighbor_id']
) }}

WITH unioned AS (
    SELECT * FROM {{ ref('int_neighbor_barriers_mt') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_pa') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_ro') }}
    UNION ALL
    SELECT * FROM {{ ref('int_neighbor_barriers_am') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as consolidated_at
FROM unioned
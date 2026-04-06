-- tests/assert_adjacency_mitigation_integrity.sql

WITH neighbor_expected_weights AS (
    SELECT 
        b.property_id,
        MAX(CASE 
            WHEN f2.slave_labor_match_confidence = 'HIGH' THEN 100
            WHEN f2.embargo_area_ha_raw > 0.1 THEN 80
            WHEN f2.mapbiomas_deforested_ha_raw > 0.1 THEN 70
            WHEN f2.rl_deficit_ha > 0.01 AND f2.is_small_holder IS FALSE THEN 20
            ELSE 10
        END) as expected_max_weight
    FROM {{ ref('int_compliance__neighbor_barriers') }} b
    JOIN {{ ref('int_compliance__property_analysis') }} f2 ON b.neighbor_id = f2.property_id
    -- AJUSTE: O teste deve olhar apenas para os vizinhos que o motor considera (os bloqueados)
    WHERE f2.is_technically_blocked = TRUE
    GROUP BY 1
)

SELECT
    s.property_id,
    s.max_adjacency_score,
    e.expected_max_weight
FROM {{ ref('int_compliance__adjacency_scoring') }} s
JOIN neighbor_expected_weights e ON s.property_id = e.property_id
WHERE s.max_adjacency_score != e.expected_max_weight
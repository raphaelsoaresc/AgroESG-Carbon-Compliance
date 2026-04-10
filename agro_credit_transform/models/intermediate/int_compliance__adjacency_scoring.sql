{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id']
) }}

WITH neighbor_data AS (
    SELECT 
        -- Garantimos que os IDs de ambos os lados do join estejam limpos (UPPER/TRIM)
        UPPER(TRIM(b.property_id)) as property_id,
        UPPER(TRIM(b.neighbor_id)) as neighbor_id,
        b.distance_meters,
        b.contact_point,
        b.is_physically_blocked,
        b.has_road_connection,
        b.has_power_connection,
        b.connected_roads_names,
        b.connected_power_lines_names,
        b.barrier_river_name,
        
        -- Dados de risco do vizinho vindos da análise técnica atualizada
        f2.is_technically_blocked as neighbor_blocked,
        f2.slave_labor_match_confidence as neighbor_slave_labor,
        f2.embargo_area_ha_raw as neighbor_embargo_ha,
        f2.mapbiomas_deforested_ha_raw as neighbor_defo_ha,
        f2.rl_deficit_ha as neighbor_rl_deficit,
        f2.is_small_holder as neighbor_is_small
    FROM {{ ref('int_compliance__neighbor_barriers') }} b
    INNER JOIN {{ ref('int_compliance__property_analysis') }} f2 
        ON UPPER(TRIM(b.neighbor_id)) = UPPER(TRIM(f2.property_id))
    -- Focamos apenas em vizinhos que possuem bloqueios técnicos (Verdade de Solo)
    WHERE f2.is_technically_blocked = TRUE
),

scoring AS (
    SELECT
        property_id,
        neighbor_id,
        contact_point,
        is_physically_blocked,
        has_road_connection,
        has_power_connection,
        connected_roads_names,
        connected_power_lines_names,
        barrier_river_name,
        
        -- 1. Definição do Peso Base (Sincronizado com o teste assert_adjacency_mitigation_integrity)
        CASE 
            WHEN neighbor_slave_labor = 'HIGH' THEN 100
            WHEN neighbor_embargo_ha > 0.1 THEN 80
            WHEN neighbor_defo_ha > 0.1 THEN 70
            WHEN neighbor_rl_deficit > 0.01 AND neighbor_is_small IS FALSE THEN 20
            ELSE 10
        END as base_weight,

        -- 2. Rótulo do Risco para o Laudo Forense
        CASE
            WHEN has_road_connection THEN 'LAUNDERING_RISK_ROAD'
            WHEN has_power_connection THEN 'LAUNDERING_RISK_POWER'
            WHEN neighbor_slave_labor = 'HIGH' THEN 'ADJACENT_SOCIAL_RISK'
            WHEN neighbor_embargo_ha > 0.1 THEN 'ADJACENT_EMBARGO'
            WHEN neighbor_defo_ha > 0.1 THEN 'ADJACENT_DEFORESTATION'
            ELSE 'ADJACENT_OTHER_RISK'
        END as risk_label
    FROM neighbor_data
),

aggregated AS (
    SELECT
        property_id,
        -- O score máximo entre todos os vizinhos (bruto, antes da mitigação por barreira física)
        MAX(base_weight) as max_adjacency_score,
        
        -- Flags consolidadas de barreiras e conexões
        LOGICAL_OR(is_physically_blocked) as has_physical_barrier,
        LOGICAL_OR(has_road_connection) as has_road_adjacency,
        LOGICAL_OR(has_power_connection) as has_power_adjacency,

        -- Evidências Agregadas para o Mart
        STRING_AGG(DISTINCT risk_label, ' | ') as adjacency_risk_types,
        STRING_AGG(DISTINCT connected_roads_names, ' | ') as adjacent_roads,
        STRING_AGG(DISTINCT connected_power_lines_names, ' | ') as adjacent_power_lines,
        STRING_AGG(DISTINCT barrier_river_name, ' | ') as adjacent_rivers,

        -- Ponto de evidência: Pegamos o ponto de contato geográfico do vizinho mais perigoso
        ARRAY_AGG(contact_point ORDER BY base_weight DESC LIMIT 1)[OFFSET(0)] as critical_contact_point
    FROM scoring
    GROUP BY 1
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as processed_at
FROM aggregated
{{ config(
    materialized='table',
    cluster_by=['property_id']
) }}

WITH neighbor_data AS (
    SELECT 
        b.property_id,
        b.neighbor_id,
        b.distance_meters,
        b.is_physically_blocked, -- Mantido para evidência no Mart
        b.has_road_connection,   -- Mantido para evidência no Mart
        b.connected_road_name,
        f2.is_technically_blocked as neighbor_blocked,
        f2.slave_labor_match_confidence as neighbor_slave_labor,
        f2.embargo_fixed_ha as neighbor_embargo_ha,
        f2.defo_fixed_ha as neighbor_defo_ha,
        f2.rl_deficit_ha as neighbor_rl_deficit,
        f2.is_small_holder as neighbor_is_small,
        f1.rl_balance_ha as principal_rl_balance
    FROM {{ ref('int_compliance__neighbor_barriers') }} b
    -- Join com a análise da fazenda principal
    INNER JOIN {{ ref('int_compliance__property_analysis') }} f1 ON b.property_id = f1.property_id
    -- Join com a análise do vizinho
    INNER JOIN {{ ref('int_compliance__property_analysis') }} f2 ON b.neighbor_id = f2.property_id
    -- Só calculamos risco se o vizinho estiver bloqueado (Verdade de Solo)
    WHERE f2.is_technically_blocked = TRUE
),

scoring AS (
    SELECT
        property_id,
        neighbor_id,
        connected_road_name,
        -- ADICIONE ESTAS DUAS LINHAS ABAIXO:
        is_physically_blocked,
        has_road_connection,
        
        -- Definição do Peso Base
        CASE 
            WHEN neighbor_slave_labor = 'HIGH' THEN 100
            WHEN neighbor_embargo_ha > 0.1 THEN 80
            WHEN neighbor_defo_ha > 0.1 THEN 70
            WHEN neighbor_rl_deficit > 0.01 AND neighbor_is_small IS FALSE THEN 20
            ELSE 10
        END as base_weight,

        -- Rótulo do Risco
        CASE
            WHEN has_road_connection THEN 'LAUNDERING_RISK'
            WHEN neighbor_slave_labor IS NOT NULL THEN 'SOCIAL'
            WHEN neighbor_embargo_ha > 0.1 THEN 'EMBARGO'
            WHEN neighbor_defo_ha > 0.1 THEN 'DESMATAMENTO'
            WHEN neighbor_rl_deficit > 0.01 THEN 'RL_DEFICIT'
            ELSE 'OUTROS'
        END as risk_label
    FROM neighbor_data
)
SELECT
    property_id,
    -- O score agora é o risco real e bruto do vizinho
    MAX(base_weight) as max_adjacency_score,
    
    -- EVIDÊNCIAS PARA O PLANO 3 (Adicione estas linhas):
    LOGICAL_OR(is_physically_blocked) as has_physical_barrier,
    LOGICAL_OR(has_road_connection) as has_road_adjacency,

    -- Lista de riscos para o laudo
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT risk_label IGNORE NULLS), ' | ') as adjacency_risk_types,
    
    -- Evidência Logística para o Frontend: "Conectado pela BR-163"
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT connected_road_name IGNORE NULLS), ' | ') as adjacent_roads
FROM scoring
GROUP BY 1
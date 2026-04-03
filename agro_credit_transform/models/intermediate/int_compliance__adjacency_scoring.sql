-- models/intermediate/int_compliance__adjacency_scoring.sql
{{ config(
    materialized='table',
    cluster_by=['property_id']
) }}

WITH neighbor_data AS (
    SELECT 
        b.property_id,
        b.neighbor_id,
        b.shared_boundary_meters,
        b.is_physically_blocked,
        f2.is_technically_blocked as neighbor_blocked,
        f2.slave_labor_match_confidence as neighbor_slave_labor,
        -- AJUSTE: Usando os nomes de colunas sincronizados com o int_property_analysis
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
    -- Só calculamos risco se o vizinho estiver bloqueado
    WHERE f2.is_technically_blocked = TRUE
),

scoring AS (
    SELECT
        property_id,
        neighbor_id,
        -- Definição do Peso Base (Severidade do vizinho) - Regra Original Mantida
        CASE 
            WHEN neighbor_slave_labor = 'HIGH' THEN 100
            WHEN neighbor_embargo_ha > 0.1 THEN 80
            WHEN neighbor_defo_ha > 0.1 THEN 70
            WHEN neighbor_rl_deficit > 0.01 AND neighbor_is_small IS FALSE THEN 20
            ELSE 10
        END as base_weight,
        
        -- Lógica de Multiplicador (Mitigação de Adjacência) - Regra Original Mantida
        CASE 
            -- 1. Mitigação Total: Se o vizinho só tem déficit de RL e a principal tem sobra para compensar
            WHEN (principal_rl_balance > neighbor_rl_deficit AND neighbor_defo_ha <= 0.1 AND neighbor_slave_labor IS NULL) THEN 0
            
            -- 2. Mitigação por Barreira Física: Se houver rio/lago separando (Reduz 90% do risco)
            WHEN is_physically_blocked THEN 0.1
            
            -- 3. Mitigação por Fronteira Curta: Contato menor que 100m (Reduz 50% do risco)
            WHEN shared_boundary_meters < 100 THEN 0.5
            
            ELSE 1.0
        END as multiplier,

        -- Rótulo do Risco para o relatório - Regra Original Mantida
        CASE
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
    -- O score final é o maior risco encontrado entre todos os vizinhos
    MAX(base_weight * multiplier) as max_adjacency_score,
    -- Lista distinta de quais tipos de riscos os vizinhos apresentam
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT risk_label IGNORE NULLS), ' | ') as adjacency_risk_types
FROM scoring
GROUP BY 1
-- tests/assert_adjacency_mitigation_integrity.sql

WITH neighbor_risk_evaluation AS (
    -- 1. Recalculamos o peso base de cada vizinho usando as colunas refatoradas
    SELECT 
        b.property_id,
        b.neighbor_id,
        b.is_physically_blocked,
        CASE 
            WHEN f2.slave_labor_match_confidence = 'HIGH' THEN 100
            WHEN f2.embargo_area_ha_raw > 0.1 THEN 80
            WHEN f2.mapbiomas_deforested_ha_raw > 0.1 THEN 70
            ELSE 0
        END as base_weight
    FROM {{ ref('int_compliance__neighbor_barriers') }} b
    JOIN {{ ref('int_compliance__property_analysis') }} f2 ON b.neighbor_id = f2.property_id
)

SELECT
    s.property_id,
    s.max_adjacency_score
FROM {{ ref('int_compliance__adjacency_scoring') }} s
WHERE 
    -- O teste falha se encontrarmos uma propriedade onde:
    
    -- A) O score final ainda é alto (indicando risco crítico)
    s.max_adjacency_score >= 70
    
    -- B) E não existe NENHUM vizinho que justifique esse score alto sem estar bloqueado.
    -- Ou seja: se todos os vizinhos que poderiam causar score >= 70 estão com is_physically_blocked = TRUE,
    -- o score final de 's' obrigatoriamente deveria ser menor que 70 (devido ao multiplicador de 0.1).
    AND NOT EXISTS (
        SELECT 1 
        FROM neighbor_risk_evaluation n
        WHERE n.property_id = s.property_id 
          AND n.base_weight >= 70 
          AND n.is_physically_blocked = FALSE
    )
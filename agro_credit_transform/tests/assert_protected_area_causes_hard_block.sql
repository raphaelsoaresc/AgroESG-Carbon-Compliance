SELECT 
    property_id,
    final_eligibility_status,
    forensic_ti_ha,
    forensic_uc_ha,
    forensic_quilombo_ha,
    forensic_settlement_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    (
        -- Caso 1: Áreas Restritas (TI, UC, Quilombo) 
        -- Só falha se não estiver bloqueado E não for a identidade confirmada E não for revisão manual
        ((forensic_ti_ha > 0.01 OR forensic_uc_ha > 0.01 OR forensic_quilombo_ha > 0.01) 
          AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
          AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW_REQUIRED%' -- ADICIONADO
          AND final_eligibility_status NOT IN (
              'ELIGIBLE - INDIGENOUS PRODUCER', 
              'ELIGIBLE - CONSERVATION UNIT PRODUCER', 
              'ELIGIBLE - QUILOMBOLA PRODUCER'
          ))
        
        OR
        
        -- Caso 2: Assentamentos/Tradicionais
        ((forensic_settlement_ha > 0.01 OR forensic_traditional_ha > 0.01)
          AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
          AND final_eligibility_status NOT LIKE 'ELIGIBLE - % PRODUCER'
          AND final_eligibility_status NOT LIKE 'WARNING%'
          AND final_eligibility_status NOT LIKE 'CONDITIONAL%'
          AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW%')
    )
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
        -- Caso 1: Áreas Restritas (TI, UC, Quilombo) - Bloqueio é obrigatório
        ((forensic_ti_ha > 0.01 OR forensic_uc_ha > 0.01 OR forensic_quilombo_ha > 0.01) 
          AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%')
        
        OR
        
        -- Caso 2: Assentamentos/Tradicionais - Só podem passar se forem Identidade (Produtor) ou Alertas
        ((forensic_settlement_ha > 0.01 OR forensic_traditional_ha > 0.01)
          AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
          AND final_eligibility_status NOT LIKE 'ELIGIBLE - % PRODUCER'
          AND final_eligibility_status NOT LIKE 'WARNING%'
          AND final_eligibility_status NOT LIKE 'CONDITIONAL%'
          AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW%')
    )
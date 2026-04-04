SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Erro 1: Está bloqueado tecnicamente no booleano, mas o status final é ELIGIBLE puro (sem ser produtor validado)
    (is_technically_blocked = TRUE AND final_eligibility_status = 'ELIGIBLE')
    
    OR
    
    -- Erro 2: Status de Invasão sem ter área de sobreposição real
    (final_eligibility_status LIKE 'NOT ELIGIBLE%INVASION%' 
     AND forensic_settlement_ha = 0 
     AND forensic_traditional_ha = 0 
     AND forensic_ti_ha = 0 
     AND forensic_uc_ha = 0 
     AND forensic_quilombo_ha = 0)

    OR

    -- Erro 3: Tem sobreposição em TI, UC ou Quilombo, mas o status não reflete restrição, identidade ou revisão
    ((forensic_ti_ha > 0.1 OR forensic_uc_ha > 0.1 OR forensic_quilombo_ha > 0.1) 
     AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
     AND final_eligibility_status NOT LIKE 'ELIGIBLE - % PRODUCER'
     AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW_REQUIRED%')
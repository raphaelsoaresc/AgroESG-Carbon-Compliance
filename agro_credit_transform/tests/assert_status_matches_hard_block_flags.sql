SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Erro 1: Está bloqueado tecnicamente mas o status é ELIGIBLE puro
    (is_technically_blocked = TRUE AND final_eligibility_status = 'ELIGIBLE')
    
    OR
    
    -- Erro 2: Status de Invasão sem ter área de sobreposição
    (final_eligibility_status IN ('NOT ELIGIBLE - SETTLEMENT (INVASION)', 'NOT ELIGIBLE - TRADITIONAL TERRITORY (INVASION)') 
     AND forensic_settlement_ha = 0 AND forensic_traditional_ha = 0)

    OR

    -- Erro 3: TI, UC ou Quilombo com área mas status não é NOT ELIGIBLE
    ((forensic_ti_ha > 0.1 OR forensic_uc_ha > 0.1 OR forensic_quilombo_ha > 0.1) 
     AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%')
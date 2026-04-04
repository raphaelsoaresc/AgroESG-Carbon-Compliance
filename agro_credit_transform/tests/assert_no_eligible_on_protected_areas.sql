SELECT 
    property_id,
    final_eligibility_status,
    forensic_ti_ha,
    forensic_uc_ha,
    forensic_quilombo_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE (forensic_ti_ha > 0.1 OR forensic_uc_ha > 0.1 OR forensic_quilombo_ha > 0.1)
    -- O teste só falha se NÃO for bloqueio, NÃO for revisão manual e NÃO for identidade legítima
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
    AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW_REQUIRED%'
    AND final_eligibility_status NOT IN (
        'ELIGIBLE - INDIGENOUS PRODUCER', 
        'ELIGIBLE - CONSERVATION UNIT PRODUCER', 
        'ELIGIBLE - QUILOMBOLA PRODUCER',
        'ELIGIBLE - SETTLEMENT PRODUCER',
        'ELIGIBLE - TRADITIONAL PRODUCER'
    )
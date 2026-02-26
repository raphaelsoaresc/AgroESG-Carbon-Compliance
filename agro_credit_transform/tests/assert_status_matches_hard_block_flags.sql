SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Se o status diz que invadiu área protegida, a flag TEM que ser TRUE
    (final_eligibility_status = 'NOT ELIGIBLE - PROTECTED AREA INVASION' AND is_protected_area_overlap = FALSE)
    
    OR
    
    -- Se a flag é TRUE e a área é relevante (>0.1), o status NÃO pode ser Elegível
    (is_protected_area_overlap = TRUE AND protected_overlap_ha > 0.1 AND final_eligibility_status LIKE 'ELIGIBLE%')
SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Erro 1: É elegível mas tem embargo real (> 0.1 ha)
    (final_eligibility_status = 'ELIGIBLE' AND embargo_area_ha > 0.1)
    
    OR
    
    -- Erro 2: O status diz que é violação de EMBARGO, mas a data está nula
    (final_eligibility_status LIKE '%EMBARGO%' AND embargo_date IS NULL)
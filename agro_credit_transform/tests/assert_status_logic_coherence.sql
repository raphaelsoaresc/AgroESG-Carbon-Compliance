-- Falha se o status for ELIGIBLE mas houver sobreposição significativa (> 0.1 ha)
-- Ou se o status for de VIOLAÇÃO mas não houver data de embargo

SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    (eligibility_status = 'ELIGIBLE' AND embargo_overlap_ha > 0.1)
    OR
    (eligibility_status LIKE '%VIOLATION%' AND embargo_date IS NULL)
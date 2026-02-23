SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Erro 1: É elegível mas tem embargo real
    (eligibility_status = 'ELIGIBLE' AND embargo_overlap_ha > 0.1)
    OR
    -- Erro 2: É uma violação de EMBARGO mas não tem data (Ignora violações de TI/Quilombo aqui)
    (eligibility_status LIKE '%VIOLATION%' AND final_embargo_date IS NULL)
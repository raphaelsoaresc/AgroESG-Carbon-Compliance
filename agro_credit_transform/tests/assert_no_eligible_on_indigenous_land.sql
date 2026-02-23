SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    is_indigenous_land = TRUE 
    AND eligibility_status LIKE 'ELIGIBLE%'
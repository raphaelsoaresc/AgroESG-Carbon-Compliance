SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    is_quilombola_land = TRUE 
    AND eligibility_status LIKE 'ELIGIBLE%'
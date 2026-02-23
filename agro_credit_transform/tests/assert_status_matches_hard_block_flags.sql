SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    (eligibility_status = 'NOT ELIGIBLE - INDIGENOUS LAND OVERLAP' AND is_indigenous_land = FALSE)
    OR
    (eligibility_status = 'NOT ELIGIBLE - QUILOMBOLA TERRITORY OVERLAP' AND is_quilombola_land = FALSE)
SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    biome_name LIKE 'AMAZ%NIA'
    AND embargo_date >= '2008-07-22'
    AND embargo_area_ha >= 0.001 -- Tolerância quase zero
    
    -- O teste falha se o status for "ELIGIBLE". 
    -- Se for "WARNING - MICRO EMBARGO", o teste passa, pois o risco foi mitigado.
    AND final_eligibility_status LIKE 'ELIGIBLE%'
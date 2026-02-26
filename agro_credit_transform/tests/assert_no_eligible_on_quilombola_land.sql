-- Garante que ninguém marcado como ELIGIBLE tenha sobreposição com Quilombo
SELECT f.*
FROM {{ ref('fct_compliance_risk') }} f
JOIN {{ ref('int_car_spatial_restrictions') }} r ON f.property_id = r.property_id
WHERE 
    f.final_eligibility_status LIKE 'ELIGIBLE%'
    AND EXISTS (
        SELECT 1 FROM UNNEST(r.overlaps_details) d 
        WHERE d.restriction_type = 'QUILOMBOLA'
    )
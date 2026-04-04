-- Garante que ninguém marcado como ELIGIBLE (puro) tenha sobreposição RELEVANTE com Quilombo
SELECT f.*
FROM {{ ref('fct_compliance_risk') }} f
JOIN {{ ref('int_car_spatial_restrictions') }} r ON f.property_id = r.property_id
WHERE 
    f.final_eligibility_status LIKE 'ELIGIBLE%'
    AND f.final_eligibility_status != 'ELIGIBLE - QUILOMBOLA PRODUCER'
    AND f.final_eligibility_status NOT LIKE 'MANUAL_REVIEW_REQUIRED%'
    AND EXISTS (
        SELECT 1 FROM UNNEST(r.overlaps_details) d 
        WHERE d.restriction_type = 'QUILOMBOLA'
        -- Alinhamento com a regra de negócio: ignorar ruído menor que 0.01ha
        AND d.overlap_ha > 0.01 
    )
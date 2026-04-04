SELECT f.*
FROM {{ ref('fct_compliance_risk') }} f
JOIN {{ ref('int_car_spatial_restrictions') }} r ON f.property_id = r.property_id
WHERE 
    -- O erro é ser ELIGIBLE comum (privado) em terra indígena. 
    -- Se for 'ELIGIBLE - INDIGENOUS PRODUCER', o teste deve ignorar.
    f.final_eligibility_status = 'ELIGIBLE' 
    AND EXISTS (
        SELECT 1 FROM UNNEST(r.overlaps_details) d 
        WHERE d.restriction_type = 'INDIGENOUS_LAND'
          AND d.overlap_ha > {{ var('gis_noise_ha_threshold') }}
    )
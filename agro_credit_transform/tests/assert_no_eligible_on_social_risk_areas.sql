WITH risk_areas AS (
    SELECT DISTINCT car_property_id
    FROM {{ ref('int_compliance__final_spatial_check') }}
    WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
),
mart AS (
    SELECT property_id, final_eligibility_status
    FROM {{ ref('fct_compliance_risk') }}
)
SELECT m.property_id, m.final_eligibility_status
FROM mart m
INNER JOIN risk_areas r ON m.property_id = r.car_property_id
WHERE m.final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
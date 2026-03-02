-- Se o CAR está Cancelado ou Suspenso, o status TEM que ser 'NOT ELIGIBLE - CAR STATUS'
SELECT 
    property_id,
    car_status,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE car_status IN ('CANCELADO', 'SUSPENSO')
  AND final_eligibility_status != 'NOT ELIGIBLE - CAR STATUS'
-- Se o NDVI da APP é menor que 0.4 (degradado), não pode ser ELIGIBLE.
SELECT 
    property_id,
    app_ndvi_mean,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE app_ndvi_mean < 0.4 
  AND app_ndvi_mean > 0 -- ignora nulos/nuvens
  AND final_eligibility_status = 'ELIGIBLE'
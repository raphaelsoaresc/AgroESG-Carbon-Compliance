-- Garante a integridade dos dados:
-- Se o status é 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)', 
-- então DEVEMOS ter a área e a data do desmatamento preenchidas.

WITH ghost_alerts AS (
    SELECT 
        property_id,
        final_eligibility_status,
        mapbiomas_deforested_ha,
        mapbiomas_date
    FROM {{ ref('fct_compliance_risk') }}
    WHERE final_eligibility_status LIKE '%MAPBIOMAS%'
      AND (
          mapbiomas_deforested_ha IS NULL 
          OR mapbiomas_deforested_ha <= 0
          OR mapbiomas_date IS NULL
      )
)

SELECT * FROM ghost_alerts
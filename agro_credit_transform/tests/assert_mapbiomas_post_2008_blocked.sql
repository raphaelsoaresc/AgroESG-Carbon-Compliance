-- Garante que NENHUMA propriedade marcada como 'ELIGIBLE' 
-- tenha desmatamento confirmado pelo MapBiomas após o Marco Temporal.
-- Regra: Se tem desmatamento > ruído (0.1ha) E data >= 2008, NÃO PODE SER ELIGIBLE.

WITH mapbiomas_violations AS (
    SELECT 
        property_id,
        final_eligibility_status,
        mapbiomas_deforested_ha,
        mapbiomas_date
    FROM {{ ref('fct_compliance_risk') }}
    WHERE final_eligibility_status = 'ELIGIBLE'
      AND mapbiomas_deforested_ha > 0.1 -- Ignora ruído de GIS
      AND mapbiomas_date >= '2008-07-22' -- Marco Temporal
)

SELECT * FROM mapbiomas_violations
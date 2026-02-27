-- Teste de Sanidade Matemática.
-- Garante que os cálculos geoespaciais não resultam em áreas negativas impossíveis na vida real.

SELECT 
    property_id,
    property_area_ha,
    embargo_area_ha,
    protected_overlap_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE property_area_ha < 0 
    OR embargo_area_ha < 0 
    OR protected_overlap_ha < 0
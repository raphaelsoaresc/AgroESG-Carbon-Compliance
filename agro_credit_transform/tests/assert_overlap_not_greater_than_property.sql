-- Falha se a área de sobreposição for maior que a área total da propriedade
-- (Com tolerância de 0.1ha para erros de ponto flutuante)

SELECT 
    property_alias,
    property_area_ha,
    embargo_overlap_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE embargo_overlap_ha > (property_area_ha + 0.1)
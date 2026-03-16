-- Garante que nenhuma métrica de sobreposição seja maior que a área total da fazenda
SELECT
    property_id,
    area_ha,
    protected_area_overlap_ha,
    mapbiomas_deforested_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE
    protected_area_overlap_ha > (area_ha + 0.01)
    OR mapbiomas_deforested_ha > (area_ha + 0.01)
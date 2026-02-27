-- Garante que propriedades com invasão confirmada em áreas protegidas restritas (> 0.1 ha)
-- (Terras Indígenas, Quilombolas ou Unidades de Conservação) sejam sumariamente bloqueadas.

SELECT 
    property_id,
    final_eligibility_status,
    is_protected_area_overlap,
    protected_overlap_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE is_protected_area_overlap = TRUE 
    AND protected_overlap_ha > 0.1
    -- Se o status final NÃO for 'NOT ELIGIBLE...', o teste apanha a falha de compliance
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
-- Garante que sobreposição em área protegida resulte em bloqueio (NOT_ELIGIBLE)
SELECT 
    property_id,
    protected_area_overlap_ha,
    final_eligibility_status -- ou a coluna que gera o 'verdict'
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    protected_area_overlap_ha > 0
    -- O status DEVE ser um bloqueio. Se for WARNING ou ELIGIBLE, o teste falha.
    AND final_eligibility_status NOT LIKE 'NOT_ELIGIBLE_%'
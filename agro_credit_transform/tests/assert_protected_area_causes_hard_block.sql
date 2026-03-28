SELECT 
    property_id,
    protected_area_overlap_ha,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Consideramos bloqueio obrigatório acima de 0.01 ha (para ignorar erro de milímetros no mapa)
    protected_area_overlap_ha > 0.01
    -- Se a área é protegida, o status DEVE começar com 'NOT ELIGIBLE'
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
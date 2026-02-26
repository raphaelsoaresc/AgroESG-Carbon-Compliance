-- Este teste garante que NENHUMA propriedade marcada como ELEGÍVEL
-- tenha uma inconsistência grave de dados (Embargo > Área Total).
-- Se estiver marcado como 'NOT ELIGIBLE - DATA INCONSISTENCY', o teste passa (pois o sistema funcionou).

SELECT 
    property_alias,
    property_area_ha,
    embargo_area_ha,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    embargo_area_ha > (property_area_ha + 0.5) -- Detecta a inconsistência
    AND final_eligibility_status LIKE 'ELIGIBLE%' -- Falha apenas se o sistema deixou passar
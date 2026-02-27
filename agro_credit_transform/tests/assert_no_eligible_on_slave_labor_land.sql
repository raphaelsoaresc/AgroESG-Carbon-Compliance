-- Se este teste retornar qualquer linha, o pipeline falha.
-- Objetivo: Garantir que propriedades em áreas de trabalho escravo sejam bloqueadas.

SELECT
    property_id,
    slave_labor_overlap_ha,
    final_eligibility_status
FROM {{ ref('fct_agro_esg_final_compliance') }} -- ajuste para o nome real da sua gold
WHERE 
    slave_labor_overlap_ha > 0.1 
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE - SOCIAL RISK%'
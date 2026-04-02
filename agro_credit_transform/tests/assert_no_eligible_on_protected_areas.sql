-- Garante que TI, Quilombo e UC causem bloqueio obrigatório.
-- Assentamentos e Territórios Tradicionais são validados em teste separado devido à regra de Identidade.

SELECT 
    property_id,
    final_eligibility_status,
    forensic_ti_ha,
    forensic_uc_ha,
    forensic_quilombo_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE (forensic_ti_ha > 0.1 OR forensic_uc_ha > 0.1 OR forensic_quilombo_ha > 0.1)
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
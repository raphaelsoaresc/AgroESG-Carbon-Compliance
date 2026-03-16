-- Garante que nenhuma coluna financeira tenha valor menor que zero.

SELECT 
    property_id,
    estimated_financial_liability_brl,
    liability_deforestation_brl,
    liability_rl_brl,
    liability_protected_areas_brl,
    liability_social_brl,
    liability_app_brl
FROM {{ ref('fct_compliance_risk') }}
WHERE estimated_financial_liability_brl < 0
   OR liability_deforestation_brl < 0
   OR liability_rl_brl < 0
   OR liability_protected_areas_brl < 0
   OR liability_social_brl < 0
   OR liability_app_brl < 0
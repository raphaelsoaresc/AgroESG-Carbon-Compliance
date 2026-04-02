WITH liability_check AS (
    SELECT 
        property_id,
        estimated_financial_liability_brl,
        (
            liability_deforestation_brl +
            liability_rl_brl +
            liability_protected_areas_brl +
            liability_social_brl +
            liability_app_brl +
            liability_embargo_brl  -- <--- ADICIONADO AQUI
        ) as calculated_sum
    FROM {{ ref('fct_compliance_risk') }}
)

SELECT * 
FROM liability_check
-- Usando ABS para evitar problemas de arredondamento de centavos em FLOAT64
WHERE ABS(estimated_financial_liability_brl - calculated_sum) > 0.01
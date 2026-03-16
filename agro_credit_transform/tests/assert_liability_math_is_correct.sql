-- Garante que o Passivo Total é exatamente a soma das suas partes.
-- Se isso falhar, significa que adicionamos uma nova multa no código 
-- mas esquecemos de somar na coluna final.

WITH liability_check AS (
    SELECT 
        property_id,
        estimated_financial_liability_brl,
        (
            liability_deforestation_brl +
            liability_rl_brl +
            liability_protected_areas_brl +
            liability_social_brl +
            liability_app_brl
        ) as calculated_sum
    FROM {{ ref('fct_compliance_risk') }}
)

SELECT * 
FROM liability_check
WHERE estimated_financial_liability_brl != calculated_sum
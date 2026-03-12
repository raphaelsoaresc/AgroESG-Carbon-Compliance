-- Este teste garante que a lógica de Fuzzy Match não gerou um Produto Cartesiano (Fan-Out).
-- Se um único infrator da Lista Suja for ligado a mais de 15 propriedades diferentes no SIGEF,
-- é muito provável que o nome da fazenda seja genérico demais e esteja gerando falsos positivos.

WITH fan_out_check AS (
    SELECT
        tax_id,
        employer_name,
        slave_labor_farm_name,
        COUNT(DISTINCT sigef_property_id) AS matched_sigef_count
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    
    GROUP BY 1, 2, 3
)

SELECT *
FROM fan_out_check
WHERE matched_sigef_count > 15
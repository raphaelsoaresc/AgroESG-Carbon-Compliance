-- Este teste garante que a lógica de Fuzzy Match não gerou um Produto Cartesiano (Fan-Out).
-- Se um único CPF (tax_id) da Lista Suja for ligado a mais de 15 propriedades diferentes no SIGEF,
-- é muito provável que o nome da fazenda seja genérico demais (ex: "FAZENDA SAO JOAO") 
-- e a limpeza via Regex tenha sido agressiva demais.

WITH fan_out_check AS (
    SELECT
        tax_id,
        employer_name,
        slave_labor_farm_name,
        -- Contamos quantas propriedades distintas do SIGEF foram "matchadas" para este caso
        COUNT(DISTINCT sigef_property_id) AS matched_sigef_count
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    
    -- EXCEÇÕES CONHECIDAS (Known Outliers)
    -- Ignora os CPFs que já auditámos e sabemos que geram muitos matches 
    -- (seja por loteamento real ou nome genérico sem risco)
    WHERE tax_id NOT IN ('15091515000111', '82817146620')
    
    GROUP BY 1, 2, 3
)

SELECT *
FROM fan_out_check
-- Threshold de segurança: 
-- É muito raro um empregador ter mais de 15 fazendas diferentes com o MESMO nome no mesmo estado.
WHERE matched_sigef_count > 15
-- Este teste falha se houver inconsistência entre os valores e a categoria atribuída
-- O dbt espera que este SELECT retorne ZERO linhas.

SELECT
    property_id,
    final_area_ha,
    fmp_ha,
    final_fiscal_modules,
    producer_size_category,
    is_small_holder
FROM {{ ref('int_car_properties_classified') }}
WHERE 
    -- Erro 1: Área menor que FMP mas não é Minifúndio
    (final_area_ha < fmp_ha AND producer_size_category != 'MINIFÚNDIO')
    
    -- Erro 2: Mais de 15 módulos mas não é Grande Propriedade
    OR (final_fiscal_modules > 15 AND producer_size_category != 'GRANDE PROPRIEDADE')
    
    -- Erro 3: É Small Holder mas a categoria é Média ou Grande
    OR (is_small_holder IS TRUE AND producer_size_category IN ('MÉDIA PROPRIEDADE', 'GRANDE PROPRIEDADE'))
    
    -- Erro 4: Não é Small Holder mas a categoria é Minifúndio ou Pequena
    OR (is_small_holder IS FALSE AND producer_size_category IN ('MINIFÚNDIO', 'PEQUENA PROPRIEDADE'))
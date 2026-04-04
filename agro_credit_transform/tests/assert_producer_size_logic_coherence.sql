-- Este teste valida a coerência entre a flag de Small Holder e a Categoria atribuída.
-- Ele respeita a prioridade: Minifúndio > Pequena > Média > Grande.

SELECT
    property_id,
    final_area_ha,
    fmp_ha,
    final_fiscal_modules,
    producer_size_category,
    is_small_holder
FROM {{ ref('int_car_properties_classified') }}
WHERE 
    (
        -- Erro 1: Se é Small Holder, a categoria OBRIGATORIAMENTE tem que ser Minifúndio ou Pequena
        (is_small_holder IS TRUE AND producer_size_category NOT IN ('MINIFÚNDIO', 'PEQUENA PROPRIEDADE'))
        
        -- Erro 2: Se NÃO é Small Holder, a categoria NÃO PODE ser Minifúndio ou Pequena
        OR (is_small_holder IS FALSE AND producer_size_category IN ('MINIFÚNDIO', 'PEQUENA PROPRIEDADE'))
        
        -- Erro 3: Se tem mais de 15 módulos e não é Minifúndio (prioridade), tem que ser Grande
        OR (final_fiscal_modules > 15 AND producer_size_category NOT IN ('GRANDE PROPRIEDADE', 'MINIFÚNDIO'))
        
        -- Erro 4: Se caiu na trava de 1500ha, tem que ser Grande
        OR (final_area_ha > 1500 AND producer_size_category != 'GRANDE PROPRIEDADE')
    )
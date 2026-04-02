-- tests/assert_no_massive_small_properties.sql
-- Este teste garante que imóveis gigantes não sejam classificados como Pequena Propriedade
-- mesmo que o dado de 'módulos fiscais' esteja corrompido na origem.

SELECT
    property_id,
    final_area_ha,
    producer_size_category
FROM {{ ref('int_car_properties_classified') }}
WHERE producer_size_category IN ('MINIFÚNDIO', 'PEQUENA PROPRIEDADE')
  AND final_area_ha > 1500 -- Limite de segurança (maior que qualquer 4 módulos fiscais no BR)
-- tests/assert_adjacency_risk_flagged_correctly.sql

WITH mart AS (
    SELECT 
        property_id, 
        final_eligibility_status, 
        geometry, 
        car_bbox,
        max_adjacency_score -- Adicionado para respeitar a regra de mitigação
    FROM {{ ref('fct_compliance_risk') }}
),

puramente_elegiveis AS (
    -- Propriedades que o sistema considerou totalmente limpas
    SELECT * FROM mart 
    WHERE final_eligibility_status = 'ELIGIBLE'
),

propriedades_bloqueadas AS (
    -- Propriedades que possuem restrições críticas
    SELECT * FROM mart 
    WHERE final_eligibility_status LIKE 'NOT ELIGIBLE%'
)

SELECT 
    e.property_id 
FROM puramente_elegiveis e
INNER JOIN propriedades_bloqueadas b 
    ON e.car_bbox.xmin <= b.car_bbox.xmax 
       AND e.car_bbox.xmax >= b.car_bbox.xmin 
       AND e.car_bbox.ymin <= b.car_bbox.ymax 
       AND e.car_bbox.ymax >= b.car_bbox.ymin
WHERE ST_INTERSECTS(e.geometry, b.geometry)
  AND ST_AREA(ST_INTERSECTION(e.geometry, b.geometry)) > 1
  -- REGRA DE OURO: O teste só deve falhar se o score for maior que zero 
  -- e o status ainda for 'ELIGIBLE'. Se o score for 0, a mitigação (ex: barreira física) 
  -- justificou a elegibilidade, então não é um erro.
  AND e.max_adjacency_score > 0
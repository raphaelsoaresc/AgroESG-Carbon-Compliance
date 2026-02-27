-- Teste de "Network Risk" (Risco de Contaminação por Adjacência).
-- Resgata as geometrias da camada intermediate para validar se alguma 
-- propriedade com status puramente "ELIGIBLE" toca em uma área bloqueada.

WITH mart AS (
    SELECT 
        property_id, 
        final_eligibility_status
    FROM {{ ref('fct_compliance_risk') }}
),

geometrias AS (
    SELECT 
        property_id, 
        geometry 
    FROM {{ ref('int_car_geometries') }}
),

puramente_elegiveis AS (
    SELECT 
        m.property_id, 
        g.geometry
    FROM mart m
    JOIN geometrias g ON m.property_id = g.property_id
    WHERE m.final_eligibility_status = 'ELIGIBLE'
),

propriedades_bloqueadas AS (
    SELECT 
        g.geometry
    FROM mart m
    JOIN geometrias g ON m.property_id = g.property_id
    WHERE m.final_eligibility_status LIKE 'NOT ELIGIBLE%'
)

SELECT
    e.property_id
FROM puramente_elegiveis e
INNER JOIN propriedades_bloqueadas b
    -- Validação matemática pesada de vizinhança
    ON ST_INTERSECTS(e.geometry, b.geometry)
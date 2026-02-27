-- Garante que propriedades com Cadastro Ambiental Rural (CAR) 
-- nas situações "Cancelado" ou "Suspenso" não recebam o status de Elegível.
-- Um CAR inválido inviabiliza qualquer originação de crédito.

WITH car_status AS (
    SELECT 
        property_id, 
        status_code 
    FROM {{ ref('stg_car_properties') }}
),

mart AS (
    SELECT 
        property_id, 
        final_eligibility_status
    FROM {{ ref('fct_compliance_risk') }}
)

SELECT 
    m.property_id,
    c.status_code,
    m.final_eligibility_status
FROM mart m
JOIN car_status c ON m.property_id = c.property_id
WHERE UPPER(c.status_code) IN ('CANCELADO', 'SUSPENSO', 'PENDENTE')
    AND m.final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
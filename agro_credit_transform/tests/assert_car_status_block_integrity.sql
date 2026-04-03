-- tests/assert_car_status_block_integrity.sql

SELECT 
    property_id,
    car_status,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Filtramos propriedades com CAR irregular (padronizado no Intermediate)
    car_status IN ('CANCELADO', 'SUSPENSO')
    
    -- O teste deve falhar apenas se a propriedade NÃO estiver bloqueada.
    -- Se ela estiver 'NOT ELIGIBLE - SOCIAL RISK', ela continua bloqueada, 
    -- o que satisfaz a segurança da operação.
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
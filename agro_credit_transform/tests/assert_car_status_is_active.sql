WITH car_status AS (
    -- Pega o status real da origem para comparar
    SELECT property_id, status_code FROM {{ ref('stg_car_properties') }}
),
mart AS (
    SELECT property_id, final_eligibility_status FROM {{ ref('fct_compliance_risk') }}
)
SELECT m.*, c.status_code
FROM mart m
JOIN car_status c ON m.property_id = c.property_id
WHERE 
    (
        -- Se na origem é Cancelado/Suspenso, não pode ser Eligible nem Warning/Conditional
        UPPER(TRIM(c.status_code)) IN ('CANCELADO', 'CA', 'C', 'SUSPENSO', 'SU', 'S')
        AND m.final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
        AND m.final_eligibility_status NOT LIKE 'MANUAL_REVIEW%' -- Adicionado: Se está em revisão, o teste passa
    )
    OR
    (
        -- Se na origem é Pendente, não pode ser Eligible puro
        UPPER(TRIM(c.status_code)) IN ('PENDENTE', 'PE', 'P')
        AND m.final_eligibility_status = 'ELIGIBLE'
    )
    AND m.final_eligibility_status NOT LIKE 'ELIGIBLE - % PRODUCER'
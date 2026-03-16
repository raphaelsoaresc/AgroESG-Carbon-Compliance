-- Garante que não exista déficit se a fazenda for isenta (EXEMPT)
SELECT
    property_id,
    rl_status,
    rl_deficit_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE
    rl_status = 'EXEMPT'
    AND rl_deficit_ha > 0
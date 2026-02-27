-- Este teste verifica a integridade da cadeia de Risco Social.
-- Falha se existirem propriedades marcadas como 'SOCIAL_RISK_SLAVE_LABOR' na tabela final
-- que vieram de um match territorial considerado de 'BAIXA' confiança (LOW) na ponte.
-- Isso evita bloquear produtores inocentes baseados em matches fracos de nome.

WITH risk_data AS (
    SELECT 
        tax_id,
        employer_name,
        risk_type
    FROM {{ ref('int_compliance__final_spatial_check') }}
    WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
),

bridge_data AS (
    SELECT 
        tax_id,
        territorial_match_confidence
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
)

SELECT
    r.tax_id,
    r.employer_name,
    b.territorial_match_confidence
FROM risk_data r
INNER JOIN bridge_data b
    ON r.tax_id = b.tax_id
WHERE b.territorial_match_confidence = 'LOW'
-- Garante que nenhuma análise no Mart tenha mais de 90 dias.
-- Se houver, o teste falha para avisar que a DAG do Airflow precisa rodar para esses grids.
SELECT 
    property_id,
    analyzed_at
FROM {{ ref('fct_compliance_risk') }}
WHERE analyzed_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    -- Mudamos a fonte pois a tabela 'metadados' continha apenas dicionário de dados
    SELECT * FROM {{ source('raw_data', 'car_temas_ambientais') }}
),

renamed_and_filtered AS (
    SELECT
        registro_car as property_id,
        
        -- Registration Dates
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(data_inscricao AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(data_inscricao AS STRING)), 10))
        ) as registration_date,

        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(data_alteracao_condicao_cadastro AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(data_alteracao_condicao_cadastro AS STRING)), 10))
        ) as last_update_date,
        
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(data_ultima_retificacao AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(data_ultima_retificacao AS STRING)), 10))
        ) as rectification_date,

        situacao_cadastro as registration_status,
        condicao_cadastro as registration_condition,
        
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY property_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
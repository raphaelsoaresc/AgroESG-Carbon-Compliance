{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'car_temas_ambientais') }}
),

renamed_and_filtered AS (
    SELECT
        registro_car as property_id,
        
        -- Registration Dates (Mantendo sua lógica original)
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
        
        -- Colunas de Localização e Atributos
        uf,
        municipio,
        codigo_ibge,
        tipo_imovel_rural,
        solicitacao_adesao_pra,
        source_filename,
        SAFE_CAST(latitude AS FLOAT64) as latitude,
        SAFE_CAST(longitude AS FLOAT64) as longitude,
        SAFE_CAST(modulos_fiscais AS FLOAT64) as modulos_fiscais,

        -- APLICAÇÃO DA TRAVA DE SANIDADE (m² para ha)
        CASE 
            WHEN SAFE_CAST(area_do_imovel AS FLOAT64) > 10000
            THEN SAFE_CAST(area_do_imovel AS FLOAT64) / 10000
            ELSE SAFE_CAST(area_do_imovel AS FLOAT64)
        END as area_do_imovel,

        CASE 
            WHEN SAFE_CAST(area_liquida AS FLOAT64) > 10000
            THEN SAFE_CAST(area_liquida AS FLOAT64) / 10000
            ELSE SAFE_CAST(area_liquida AS FLOAT64)
        END as area_liquida,

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
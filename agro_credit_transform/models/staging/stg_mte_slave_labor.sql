{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['compliance', 'slave_labor']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'mte_slave_labor_history') }}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                cnpjcpf, 
                estabelecimento, 
                inclusao_no_cadastro_de_empregadores
            ORDER BY ingested_at DESC
        ) as row_num
    FROM source_data
),

renamed_and_filtered AS (
    SELECT
        -- Metadados de Auditoria
        file_hash,
        ingested_at,
        source_filename,

        -- Identificação
        id as source_id,
        UPPER(TRIM(empregador)) as employer_name,
        
        -- Limpeza de CPF/CNPJ: Remove pontos, traços e barras
        REGEXP_REPLACE(cnpjcpf, r'[\.\-\/]', '') as tax_id,
        
        -- Localização e Imóvel
        UPPER(TRIM(uf)) as state,
        UPPER(TRIM(estabelecimento)) as property_location_raw,
        
        -- Dados da Infração
        SAFE_CAST(ano_da_acao_fiscal AS INT64) as fiscal_year,
        SAFE_CAST(trabalhadores_envolvidos AS INT64) as workers_count,
        UPPER(TRIM(cnae)) as economic_activity_code,

        -- Datas: Seguindo o padrão COALESCE do seu modelo IBAMA
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(inclusao_no_cadastro_de_empregadores AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(inclusao_no_cadastro_de_empregadores AS STRING)), 10))
        ) as inclusion_date,

        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(decisao_administrativa_de_procedencia AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(decisao_administrativa_de_procedencia AS STRING)), 10))
        ) as administrative_decision_date

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed_and_filtered
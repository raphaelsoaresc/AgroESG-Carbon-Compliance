{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='tax_id',
    tags=['compliance']
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_mte_slave_labor') }}
),

-- 1. Extração via Regex da Cidade e UF (que costumam estar no final da string)
extraction AS (
    SELECT
        *,
        -- Pega a UF (Últimas duas letras após uma barra ou traço no final da string)
        REGEXP_EXTRACT(property_location_raw, r'([A-Z]{2})\s*$') as extracted_uf,
        
        -- Pega a Cidade (Texto entre a última vírgula e a barra/traço da UF)
        TRIM(REGEXP_EXTRACT(property_location_raw, r',\s*([^,/-]+)[/-][A-Z]{2}\s*$')) as extracted_city,
        
        -- Pega o Nome do Imóvel (Tudo antes da primeira vírgula)
        TRIM(REGEXP_EXTRACT(property_location_raw, r'^([^,]+)')) as extracted_farm_name
    FROM staging_data
),

-- 2. Normalização do Nome da Fazenda para o Match (Remoção de ruído)
normalization AS (
    SELECT
        * EXCEPT(extracted_farm_name),
        extracted_farm_name as farm_name_raw,
        -- Limpeza profunda para chave de match:
        -- Remove acentos, termos genéricos e espaços extras
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                NORMALIZE(UPPER(extracted_farm_name), NFD), 
            r"\pM", ""), 
        r'\b(FAZENDA|SITIO|ESTANCIA|GLEBA|CHACARA|PROPRIEDADE|RURAL|AGROPECUARIA|RESIDENCIA|OBRA)\b', '') as farm_name_match_key
    FROM extraction
),

-- 3. Refinamento Final e Flags de Qualidade
final_processing AS (
    SELECT
        * EXCEPT(farm_name_match_key),
        TRIM(REGEXP_REPLACE(farm_name_match_key, r'\s+', ' ')) as farm_name_match_key,
        
        -- Flag para identificar se é um registro Rural ou Urbano
        CASE 
            WHEN REGEXP_CONTAINS(farm_name_raw, r'(FAZENDA|SITIO|GLEBA|CHACARA|ZONA RURAL)') THEN TRUE
            ELSE FALSE
        END as is_rural_target,

        -- Flag de qualidade da extração
        CASE 
            WHEN extracted_city IS NOT NULL AND extracted_uf IS NOT NULL THEN 'HIGH'
            WHEN extracted_uf IS NOT NULL THEN 'MEDIUM'
            ELSE 'LOW'
        END as extraction_confidence
    FROM normalization
)

SELECT * FROM final_processing
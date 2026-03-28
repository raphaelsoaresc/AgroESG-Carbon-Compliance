{{ config(materialized='table', schema='agro_esg_intermediate') }}

WITH staging_mte AS (
    SELECT
        tax_id,
        employer_name,
        state as mte_uf,
        property_location_raw, -- Esta é a coluna 'estabelecimento'
        inclusion_date
    FROM {{ ref('stg_mte_slave_labor') }}
),

extraction AS (
    SELECT
        *,
        -- Pega tudo antes da primeira vírgula (Geralmente o nome da Fazenda)
        -- Ex: "FAZENDA SANTA LUCIA, ZONA RURAL..." -> "FAZENDA SANTA LUCIA"
        UPPER(TRIM(REGEXP_EXTRACT(property_location_raw, r'^([^,]+)'))) as extracted_farm_name
    FROM staging_mte
)

SELECT
    tax_id,
    employer_name,
    mte_uf,
    inclusion_date,
    property_location_raw,
    -- Normalização para o Match (Remove termos genéricos para aumentar a chance de acerto)
    UPPER(TRIM(REGEXP_REPLACE(NORMALIZE(extracted_farm_name, NFD), r"\pM", ""))) as farm_name_normalized,
    -- Remove palavras como FAZENDA, SITIO para sobrar o nome próprio
    REGEXP_REPLACE(
        UPPER(TRIM(REGEXP_REPLACE(NORMALIZE(extracted_farm_name, NFD), r"\pM", ""))),
        r'\b(FAZENDA|SITIO|ESTANCIA|GLEBA|CHACARA|PROPRIEDADE|RURAL|AGROPECUARIA)\b', 
        ''
    ) as farm_core_name
FROM extraction
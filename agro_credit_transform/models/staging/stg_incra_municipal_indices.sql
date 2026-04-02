{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    -- Diferente do SIGEF, aqui lemos a tabela única de referência
    SELECT * FROM {{ source('raw_data', 'incra_municipal_indices') }}
),

renamed AS (
    SELECT
        -- Padronizamos os nomes para o restante do projeto
        municipio_id as city_ibge_id,
        UPPER(TRIM(municipio_nome)) as city_name,
        uf_sigla as state_ibge_id, -- Note que aqui é o código numérico (ex: 15, 51)
        CAST(modulo_fiscal_ha AS FLOAT64) as fiscal_module_ha,
        CAST(fmp_ha AS FLOAT64) as fmp_ha
    FROM source_data
),

-- Barreira de Qualidade: Garante que não existam códigos IBGE duplicados
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_ibge_id 
            ORDER BY city_ibge_id
        ) as rn
    FROM renamed
)

SELECT 
    * EXCEPT(rn)
FROM deduplicated
WHERE rn = 1
    AND city_ibge_id IS NOT NULL
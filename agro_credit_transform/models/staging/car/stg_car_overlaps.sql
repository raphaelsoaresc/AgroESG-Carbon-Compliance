{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['car']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'car_sobreposicao') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identificação Básica
        registro_car as property_id,
        uf,
        municipio as city_name,
        codigo_ibge as city_ibge_id,
        
        -- Metadados do Imóvel
        SAFE_CAST(area_do_imovel AS FLOAT64) as property_area_ha,
        situacao_cadastro as registration_status,
        condicao_cadastro as registration_condition,
        tipo_imovel_rural as property_type,
        SAFE_CAST(modulos_fiscais AS FLOAT64) as fiscal_modules,
        
        -- Coordenadas
        SAFE_CAST(latitude AS FLOAT64) as latitude,
        SAFE_CAST(longitude AS FLOAT64) as longitude,

        -- Dados de Sobreposição (Mantendo nomes existentes)
        descricao as overlap_name, 
        origem as overlap_source,
        SAFE_CAST(percentual AS FLOAT64) as overlap_percentage,
        SAFE_CAST(area_de_conflito AS FLOAT64) as overlap_area_ha,
        
        -- Auditoria e Processamento
        SAFE_CAST(data_processamento AS DATE) as processing_date,
        file_hash,
        source_filename,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        -- Deduplicação por imóvel e tipo de sobreposição para evitar duplicidade de polígonos
        ROW_NUMBER() OVER (
            PARTITION BY property_id, overlap_name, overlap_source
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
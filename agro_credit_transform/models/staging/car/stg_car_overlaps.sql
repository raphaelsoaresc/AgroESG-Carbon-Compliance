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
        municipio, -- Mantido nome original para consistência com stg_car_owners
        codigo_ibge, -- Mantido nome original para consistência com stg_car_owners
        
        -- Metadados do Imóvel com Trava de Sanidade (m² para ha)
        CASE 
            WHEN SAFE_CAST(area_do_imovel AS FLOAT64) > 500000 
            THEN SAFE_CAST(area_do_imovel AS FLOAT64) / 10000
            ELSE SAFE_CAST(area_do_imovel AS FLOAT64)
        END as property_area_ha,

        situacao_cadastro as registration_status,
        condicao_cadastro as registration_condition,
        tipo_imovel_rural as property_type,
        SAFE_CAST(modulos_fiscais AS FLOAT64) as fiscal_modules,
        
        -- Coordenadas
        SAFE_CAST(latitude AS FLOAT64) as latitude,
        SAFE_CAST(longitude AS FLOAT64) as longitude,

        -- Dados de Sobreposição
        descricao as overlap_name, 
        origem as overlap_source,
        SAFE_CAST(percentual AS FLOAT64) as overlap_percentage,
        
        -- Trava de Sanidade também na área de conflito (se a área do imóvel for m², o conflito também será)
        CASE 
            WHEN SAFE_CAST(area_de_conflito AS FLOAT64) > 500000 
            THEN SAFE_CAST(area_de_conflito AS FLOAT64) / 10000
            ELSE SAFE_CAST(area_de_conflito AS FLOAT64)
        END as overlap_area_ha,
        
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
        -- Deduplicação correta: um imóvel pode ter várias sobreposições diferentes
        ROW_NUMBER() OVER (
            PARTITION BY property_id, overlap_name, overlap_source
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
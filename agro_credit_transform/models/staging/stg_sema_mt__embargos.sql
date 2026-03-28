{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'sema_mt_embargos') }}
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO ÚNICA
        CAST(OBJECTID AS STRING) as embargo_id,
        CAST(T_EMBARGO AS STRING) as tad_number,
        CAST(N_PROCESSO AS STRING) as process_number,
        
        -- IDENTIFICAÇÃO DO INFRATOR
        REGEXP_REPLACE(CAST(CPF_CNPJ AS STRING), r'[\.\-\/\,]', '') as tax_id,
        TRIM(UPPER(CAST(NOME AS STRING))) as offender_name,
        TRIM(UPPER(CAST(PROPRIEDAD AS STRING))) as property_name_raw,

        -- LOCALIZAÇÃO
        'MT' as state,
        CAST(NULL AS STRING) as city, -- Município não explícito na base histórica
        CAST(NULL AS STRING) as form_status,
        CAST(NULL AS STRING) as area_type,
        CAST(OBS AS STRING) as location_description,

        -- DATA COM TRATAMENTO DE ERRO (Reaproveitando lógica do IBAMA)
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(DAT_LAVRAT AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(DAT_LAVRAT AS STRING)), 10)),
            SAFE_CAST(LEFT(TRIM(CAST(DAT_LAVRAT AS STRING)), 10) AS DATE)
        ) as embargo_date,

        -- ÁREA E COORDENADAS
        SAFE_CAST(AREA_HA AS FLOAT64) as reported_area_ha,
        SAFE_CAST(REPLACE(CAST(COORD_X AS STRING), ',', '.') AS FLOAT64) as longitude,
        SAFE_CAST(REPLACE(CAST(COORD_Y AS STRING), ',', '.') AS FLOAT64) as latitude,

        -- GEOMETRIA (Convertendo WKT para GEOGRAPHY com correção topológica nativa do BQ)
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry,

        -- STATUS JURÍDICO (Base histórica da SEMA geralmente é ativa, a menos que cruzada com SIGA)
        FALSE as is_cancelled,
        TRUE as is_active_embargo

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY embargo_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num) 
FROM deduplicated 
WHERE row_num = 1
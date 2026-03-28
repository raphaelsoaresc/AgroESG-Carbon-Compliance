{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'siga_mt_embargos') }}
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO ÚNICA
        CAST(ID_PADRAO AS STRING) as embargo_id,
        CAST(NUMERO_AUT AS STRING) as tad_number,
        CAST(NUMERO_PRO AS STRING) as process_number,
        
        -- IDENTIFICAÇÃO DO INFRATOR
        REGEXP_REPLACE(CAST(CPFCNPJ AS STRING), r'[\.\-\/\,]', '') as tax_id,
        TRIM(UPPER(CAST(NOME_RAZAO AS STRING))) as offender_name,
        CAST(NULL AS STRING) as property_name_raw, -- Não há coluna clara de nome da propriedade

        -- LOCALIZAÇÃO
        'MT' as state,
        TRIM(UPPER(CAST(MUNICIPIO_ AS STRING))) as city,
        CAST(SITUACAO AS STRING) as form_status,
        CAST(TIPO AS STRING) as area_type,
        CAST(DESCRICAO_ AS STRING) as location_description,

        -- DATA
        DATA_DO_AU as embargo_date,

        -- ÁREA E COORDENADAS
        SAFE_CAST(QUANTIDADE AS FLOAT64) as reported_area_ha,
        SAFE_CAST(REPLACE(CAST(LONGITUDE AS STRING), ',', '.') AS FLOAT64) as longitude,
        SAFE_CAST(REPLACE(CAST(LATITUDE AS STRING), ',', '.') AS FLOAT64) as latitude,

        -- GEOMETRIA (Convertendo WKT para GEOGRAPHY com correção topológica nativa do BQ)
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry,

        -- STATUS JURÍDICO (Inferido pela situação)
        CASE WHEN UPPER(CAST(SITUACAO AS STRING)) LIKE '%CANCELADO%' THEN TRUE ELSE FALSE END as is_cancelled,
        CASE WHEN UPPER(CAST(SITUACAO AS STRING)) NOT LIKE '%CANCELADO%' THEN TRUE ELSE FALSE END as is_active_embargo

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
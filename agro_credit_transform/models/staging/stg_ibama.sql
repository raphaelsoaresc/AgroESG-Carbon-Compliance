{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibama_history') }}
    WHERE UF = 'MT'
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                CAST(NUM_LONGITUDE_TAD AS STRING), 
                CAST(NUM_LATITUDE_TAD AS STRING), 
                CAST(DAT_EMBARGO AS STRING), 
                CAST(GEOM_AREA_EMBARGADA AS STRING)
            ORDER BY ingested_at DESC
        ) as row_num
    FROM source_data
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        UF as state,
        MUNICIPIO as city,
        DES_STATUS_FORMULARIO as form_status,
        TIPO_AREA as area_type,

        -- DATA: Garante o tipo DATE
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(DAT_EMBARGO AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(DAT_EMBARGO AS STRING)), 10)),
            SAFE_CAST(LEFT(TRIM(CAST(DAT_EMBARGO AS STRING)), 10) AS DATE)
        ) as embargo_date,

        -- ÁREA: Limpeza de separadores brasileiros
        SAFE_CAST(REPLACE(REPLACE(CAST(QTD_AREA_EMBARGADA AS STRING), '.', ''), ',', '.') AS FLOAT64) as reported_area_ha,

        -- COORDENADAS: Trata o erro de múltiplos pontos (ex: -12.345.678 -> -12.345678)
        SAFE_CAST(
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(REPLACE(CAST(NUM_LONGITUDE_TAD AS STRING), ',', ''), r'\.', '')) > 5 
                THEN REGEXP_REPLACE(REPLACE(CAST(NUM_LONGITUDE_TAD AS STRING), ',', ''), r'^(\-?\d{2})\.', r'\1')
                ELSE REPLACE(REPLACE(CAST(NUM_LONGITUDE_TAD AS STRING), '.', ''), ',', '.')
            END AS FLOAT64
        ) as longitude,

        SAFE_CAST(
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(REPLACE(CAST(NUM_LATITUDE_TAD AS STRING), ',', ''), r'\.', '')) > 5 
                THEN REGEXP_REPLACE(REPLACE(CAST(NUM_LATITUDE_TAD AS STRING), ',', ''), r'^(\-?\d{2})\.', r'\1')
                ELSE REPLACE(REPLACE(CAST(NUM_LATITUDE_TAD AS STRING), '.', ''), ',', '.')
            END AS FLOAT64
        ) as latitude,

        GEOM_AREA_EMBARGADA as geometry_wkt,
        CASE WHEN DAT_DESEMBARGO IS NULL THEN TRUE ELSE FALSE END as is_active_embargo

    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM renamed_and_filtered

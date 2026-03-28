{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibama_history') }}
    WHERE uf IN ('MT', 'AM', 'RO', 'PA')
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO ÚNICA
        CAST(seq_tad AS STRING) as embargo_id,
        num_tad as tad_number,
        num_processo as process_number,
        
        -- IDENTIFICAÇÃO DO INFRATOR
        REGEXP_REPLACE(CAST(cpf_cnpj_embargado AS STRING), r'[\.\-\/\,]', '') as tax_id,
        TRIM(UPPER(CAST(nome_embargado AS STRING))) as offender_name,
        TRIM(UPPER(CAST(nome_imovel AS STRING))) as property_name_raw,

        -- LOCALIZAÇÃO
        uf as state,
        TRIM(UPPER(CAST(municipio AS STRING))) as city,
        des_status_formulario as form_status,
        tipo_area as area_type,
        des_localizacao as location_description,

        -- DATA COM TRATAMENTO DE ERRO
        COALESCE(
            SAFE.PARSE_DATE('%d/%m/%Y', LEFT(TRIM(CAST(dat_embargo AS STRING)), 10)),
            SAFE.PARSE_DATE('%Y-%m-%d', LEFT(TRIM(CAST(dat_embargo AS STRING)), 10)),
            SAFE_CAST(LEFT(TRIM(CAST(dat_embargo AS STRING)), 10) AS DATE)
        ) as raw_embargo_date,

        -- ÁREA
        SAFE_CAST(REPLACE(REPLACE(CAST(qtd_area_embargada AS STRING), '.', ''), ',', '.') AS FLOAT64) as reported_area_ha,

        -- COORDENADAS
        SAFE_CAST(
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(REPLACE(CAST(num_longitude_tad AS STRING), ',', ''), r'\.', '')) > 5 
                THEN REGEXP_REPLACE(REPLACE(CAST(num_longitude_tad AS STRING), ',', ''), r'^(\-?\d{2})\.', r'\1')
                ELSE REPLACE(REPLACE(CAST(num_longitude_tad AS STRING), '.', ''), ',', '.')
            END AS FLOAT64
        ) as longitude,

        SAFE_CAST(
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(REPLACE(CAST(num_latitude_tad AS STRING), ',', ''), r'\.', '')) > 5 
                THEN REGEXP_REPLACE(REPLACE(CAST(num_latitude_tad AS STRING), ',', ''), r'^(\-?\d{2})\.', r'\1')
                ELSE REPLACE(REPLACE(CAST(num_latitude_tad AS STRING), '.', ''), ',', '.')
            END AS FLOAT64
        ) as latitude,

        -- GEOMETRIA ATUALIZADA COM MAKE_VALID
        SAFE.ST_GEOGFROMTEXT(geom_area_embargada, make_valid => TRUE) as geometry,

        -- STATUS JURÍDICO
        CASE WHEN sit_cancelado = 'S' THEN TRUE ELSE FALSE END as is_cancelled,
        
        CASE 
            WHEN dat_desembargo IS NULL AND (sit_cancelado = 'N' OR sit_cancelado IS NULL) THEN TRUE 
            ELSE FALSE 
        END as is_active_embargo

    FROM source_data
    WHERE seq_tad IS NOT NULL -- FILTRO ADICIONADO AQUI
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY embargo_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
),

final_cleaned AS (
    SELECT
        * EXCEPT(raw_embargo_date, row_num),
        CASE 
            WHEN raw_embargo_date > CURRENT_DATE() THEN CURRENT_DATE()
            ELSE raw_embargo_date 
        END AS embargo_date
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM final_cleaned
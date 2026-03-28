{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'icmbio_embargos') }}
),

renamed_and_filtered AS (
    SELECT
        file_hash,
        ingested_at,
        
        -- IDENTIFICAÇÃO ÚNICA
        CAST(numero_emb AS STRING) as embargo_id,
        CAST(numero_ai AS STRING) as tad_number,
        CAST(processo AS STRING) as process_number,
        
        -- IDENTIFICAÇÃO DO INFRATOR
        REGEXP_REPLACE(CAST(cpf_cnpj AS STRING), r'[\.\-\/\,]', '') as tax_id,
        TRIM(UPPER(CAST(autuado AS STRING))) as offender_name,
        TRIM(UPPER(CAST(nome_uc AS STRING))) as property_name_raw,

        -- LOCALIZAÇÃO
        TRIM(UPPER(CAST(uf AS STRING))) as state,
        TRIM(UPPER(CAST(municipio AS STRING))) as city,
        CAST(julgamento AS STRING) as form_status,
        CAST(tipo_infra AS STRING) as area_type,
        CAST(desc_infra AS STRING) as location_description,

        -- DATA
        data as embargo_date,

        -- ÁREA E COORDENADAS
        SAFE_CAST(REPLACE(REPLACE(CAST(area AS STRING), '.', ''), ',', '.') AS FLOAT64) as reported_area_ha,
        CAST(NULL AS FLOAT64) as longitude,
        CAST(NULL AS FLOAT64) as latitude,

        -- GEOMETRIA
        SAFE.ST_GEOGFROMTEXT(geom, make_valid => TRUE) as geometry,

        -- STATUS JURÍDICO
        CASE WHEN UPPER(CAST(julgamento AS STRING)) LIKE '%CANCELADO%' THEN TRUE ELSE FALSE END as is_cancelled,
        CASE WHEN UPPER(CAST(julgamento AS STRING)) NOT LIKE '%CANCELADO%' THEN TRUE ELSE FALSE END as is_active_embargo

    FROM source_data
    WHERE numero_emb IS NOT NULL -- FILTRO DE ID ADICIONADO AQUI
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
  AND geometry IS NOT NULL -- FILTRO DE GEOMETRIA ADICIONADO AQUI
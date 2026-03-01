{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'sigef_history') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identificadores Únicos
        codigo_imo as property_id,
        parcela_co as parcel_id,
        
        -- Atributos da Propriedade
        nome_area as property_name,
        status as certification_status,
        situacao_i as legal_situation,
        
        -- Datas
        data_submi as submission_date,
        data_aprov as approval_date,
        registro_d as registration_date,
        
        -- Localização
        municipio_ as city_id,
        uf_id as state_id,
        
        -- Geometria (WKT vindo do DuckDB)
        geom as geometry_wkt,
        
        -- Auditoria
        file_hash,
        ingested_at

    FROM source_data

),

deduplicated AS (
    SELECT 
        *,
        -- Deduplicação pelo ID do imóvel (codigo_imo)
        ROW_NUMBER() OVER (
            PARTITION BY property_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT 
    * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
    AND property_id IS NOT NULL
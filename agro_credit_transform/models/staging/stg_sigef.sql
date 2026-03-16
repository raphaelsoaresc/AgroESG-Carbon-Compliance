{{ config(
    materialized='view',
    schema='agro_esg_staging'
) }}

-- 1. Definimos a lista de estados que já temos ingeridos
{% set ufs =['mt', 'am', 'ro', 'pa'] %}

-- 2. Fazemos um loop para unir (UNION ALL) todas as tabelas de estados
WITH source_data AS (
    {% for uf in ufs %}
        SELECT * FROM {{ source('raw_data', 'sigef_history_' ~ uf) }}
        
        -- Adiciona o UNION ALL entre os selects, exceto no último
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),

renamed_and_filtered AS (
    SELECT
        codigo_imo as property_id,
        parcela_co as parcel_id,
        nome_area as property_name,
        status as certification_status,
        situacao_i as legal_situation,
        data_submi as submission_date,
        data_aprov as approval_date,
        registro_d as registration_date,
        municipio_ as city_id,
        uf_id as state_id,
        uf as state_abbreviation,
        geom as geometry_wkt,
        file_hash,
        source_filename,
        ingested_at
    FROM source_data
),

-- 1ª Barreira: Garante que não exista NENHUM parcel_id duplicado (pega o mais recente)
dedup_parcel AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY parcel_id 
            ORDER BY ingested_at DESC
        ) as rn_parcel
    FROM renamed_and_filtered
),

-- 2ª Barreira: Garante que não exista NENHUM property_id duplicado
dedup_property AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY property_id 
            ORDER BY ingested_at DESC
        ) as rn_property
    FROM dedup_parcel
    WHERE rn_parcel = 1 -- Já filtra as parcelas duplicadas aqui
)

SELECT 
    * EXCEPT(rn_parcel, rn_property)
FROM dedup_property
WHERE rn_property = 1
    AND property_id IS NOT NULL
    AND parcel_id IS NOT NULL
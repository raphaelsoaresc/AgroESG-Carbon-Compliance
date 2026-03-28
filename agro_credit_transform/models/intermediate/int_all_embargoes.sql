{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry'
) }}

WITH ibama AS (
    SELECT 
        embargo_id, 'IBAMA' as source, tax_id, embargo_date, 
        is_active_embargo, is_cancelled, geometry,
        -- Colunas Periciais e de Auditoria
        tad_number, process_number, offender_name, property_name_raw,
        reported_area_ha, longitude, latitude, state, city,
        form_status, area_type, location_description,
        file_hash, ingested_at
    FROM {{ ref('stg_ibama') }}
),

siga_mt AS (
    SELECT 
        embargo_id, 'SIGA_MT' as source, tax_id, embargo_date, 
        is_active_embargo, is_cancelled, geometry,
        -- Colunas Periciais e de Auditoria
        tad_number, process_number, offender_name, property_name_raw,
        reported_area_ha, longitude, latitude, state, city,
        form_status, area_type, location_description,
        file_hash, ingested_at
    FROM {{ ref('stg_siga_mt__embargos') }}
),

sema_mt AS (
    SELECT 
        embargo_id, 'SEMA_MT' as source, tax_id, embargo_date, 
        is_active_embargo, is_cancelled, geometry,
        -- Colunas Periciais e de Auditoria
        tad_number, process_number, offender_name, property_name_raw,
        reported_area_ha, longitude, latitude, state, city,
        form_status, area_type, location_description,
        file_hash, ingested_at
    FROM {{ ref('stg_sema_mt__embargos') }}
),

icmbio AS (
    SELECT 
        embargo_id, 'ICMBIO' as source, tax_id, embargo_date, 
        is_active_embargo, is_cancelled, geometry,
        -- Colunas Periciais e de Auditoria
        tad_number, process_number, offender_name, property_name_raw,
        reported_area_ha, longitude, latitude, state, city,
        form_status, area_type, location_description,
        file_hash, ingested_at
    FROM {{ ref('stg_icmbio__embargos') }}
),

unioned_embargoes AS (
    SELECT * FROM ibama
    UNION ALL
    SELECT * FROM siga_mt
    UNION ALL
    SELECT * FROM sema_mt
    UNION ALL
    SELECT * FROM icmbio
)

SELECT * FROM unioned_embargoes
WHERE geometry IS NOT NULL
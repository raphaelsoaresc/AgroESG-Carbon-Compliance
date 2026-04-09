{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['bronze', 'staging', 'ibge', 'hidrografia']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_massas_agua') }}
)

SELECT
    -- Identificadores e Nomes
    UPPER(TRIM(nome)) as water_body_name,
    UPPER(TRIM(tipomassadagua)) as water_body_type, -- Ex: Lagoa, Represa
    
    -- Atributos
    UPPER(TRIM(regime)) as water_regime, -- Ex: Permanente, Temporário
    UPPER(TRIM(dominialidade)) as jurisdiction,
    
    -- Flags
    CASE 
        WHEN UPPER(TRIM(salgada)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_salt_water,
    CASE 
        WHEN UPPER(TRIM(artificial)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_man_made,

    -- Geometria
    SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
    
    -- Auditoria
    file_hash,
    ingested_at

FROM source_data
WHERE geometry_wkt IS NOT NULL
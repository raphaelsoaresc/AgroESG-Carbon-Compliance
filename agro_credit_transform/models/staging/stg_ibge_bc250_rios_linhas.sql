{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['bronze', 'staging', 'ibge', 'hidrografia']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_rios_linhas') }}
)

SELECT
    -- Identificadores e Nomes
    UPPER(TRIM(nome)) as river_name,
    UPPER(TRIM(tipotrechodrenagem)) as drainage_type,
    
    -- Atributos
    UPPER(TRIM(regime)) as water_regime,
    CAST(larguramedia AS FLOAT64) as average_width_m,
    
    -- Flags
    CASE 
        WHEN UPPER(TRIM(navegavel)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_navigable,

    -- Geometria
    SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
    
    -- Auditoria
    file_hash,
    ingested_at

FROM source_data
WHERE geometry_wkt IS NOT NULL
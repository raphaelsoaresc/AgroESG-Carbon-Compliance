{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['bronze', 'staging', 'ibge', 'infraestrutura', 'transporte']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_pistas_pouso_l') }}
)

SELECT
    -- Identificadores e Nomes
    UPPER(TRIM(nome)) as airstrip_name,
    UPPER(TRIM(tipopista)) as airstrip_type,
    
    -- Atributos Técnicos
    UPPER(TRIM(revestimento)) as surface_type,
    UPPER(TRIM(usopista)) as usage_type,
    UPPER(TRIM(homologacao)) as certification_status,
    UPPER(TRIM(situacaofisica)) as physical_status,
    CAST(largura AS FLOAT64) as width_m,
    CAST(extensao AS FLOAT64) as length_m,
    
    -- Filtros e Flags
    CASE 
        WHEN UPPER(TRIM(operacional)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_operational,

    -- Geometria
    SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
    
    -- Auditoria
    file_hash,
    ingested_at

FROM source_data
WHERE geometry_wkt IS NOT NULL
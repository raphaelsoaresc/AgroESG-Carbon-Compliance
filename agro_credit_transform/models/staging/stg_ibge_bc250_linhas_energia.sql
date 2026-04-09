{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['bronze', 'staging', 'ibge', 'infraestrutura', 'energia']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_linhas_energia') }}
)

SELECT
    -- Identificadores e Nomes
    UPPER(TRIM(nome)) as line_name,
    UPPER(TRIM(especie)) as energy_type, -- Ex: Transmissão, Distribuição
    
    -- Atributos Técnicos
    UPPER(TRIM(situacaofisica)) as physical_status,
    CAST(largurafaixaservidao AS FLOAT64) as easement_width_m,
    
    -- Flags
    CASE 
        WHEN UPPER(TRIM(operacional)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_operational,
    CASE 
        WHEN UPPER(TRIM(sin)) IN ('SIM', '1') THEN TRUE 
        ELSE FALSE 
    END as is_national_grid, -- Sistema Interligado Nacional

    -- Geometria
    SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
    
    -- Auditoria
    file_hash,
    ingested_at

FROM source_data
WHERE geometry_wkt IS NOT NULL
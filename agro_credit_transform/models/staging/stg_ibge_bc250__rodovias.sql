{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['bronze', 'staging', 'ibge', 'infraestrutura']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'ibge_bc250_rodovias') }}
)

SELECT
    -- Identificadores e Nomes
    UPPER(TRIM(sigla)) as road_code, -- Ex: BR-163
    UPPER(TRIM(tipovia)) as road_type, -- Ex: Rodovia, Arruamento
    
    -- Atributos Técnicos
    UPPER(TRIM(revestimento)) as surface_type, -- Pavimentado, Terra, etc
    UPPER(TRIM(jurisdicao)) as jurisdiction, -- Federal, Estadual, Municipal
    UPPER(TRIM(administracao)) as administration,
    UPPER(TRIM(situacaofisica)) as physical_status, -- Em construção, Abandonada, etc
    
    -- Filtro de Existência
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
-- Filtramos apenas o que é operacional para evitar "estradas fantasma" no compliance
WHERE UPPER(TRIM(operacional)) IN ('SIM', '1')
  AND geometry_wkt IS NOT NULL
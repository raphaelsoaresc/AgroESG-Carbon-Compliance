{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='geometry'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_sigef') }}
),

spatial_cleaning AS (
    SELECT
        * EXCEPT(geometry_wkt),
        -- Aplica a limpeza validada no console:
        -- 1. Remove o prefixo ' Z '
        -- 2. Remove a terceira coordenada (altitude) usando Regex
        REGEXP_REPLACE(
            REPLACE(UPPER(geometry_wkt), ' Z ', ' '),
            r' [0-9.-]+([,)])',
            r'\1'
        ) as clean_wkt
    FROM staging_data
),

spatial_processing AS (
    SELECT
        * EXCEPT(clean_wkt),
        -- Converte para GEOGRAPHY com correção automática de topologia
        SAFE.ST_GEOGFROMTEXT(clean_wkt, make_valid => TRUE) as geometry
    FROM spatial_cleaning
)

SELECT * FROM spatial_processing
WHERE geometry IS NOT NULL
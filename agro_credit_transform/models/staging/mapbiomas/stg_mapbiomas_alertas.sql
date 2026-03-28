{{ config(
    materialized='view',
    schema='agro_esg_staging',
    tags=['mapbiomas', 'alerts']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'mapbiomas_alertas_shapes') }}
),

renamed_and_filtered AS (
    SELECT
        -- Identificadores
        CAST(ALERTID AS INT64) as alert_id,
        CAST(ALERTCODE AS INT64) as alert_code,
        
        -- Datas (Originais)
        -- Nota: Como no seu schema DETECTAT já é DATE, o SAFE_CAST garante compatibilidade
        SAFE_CAST(DETECTAT AS DATE) as detection_date,
        CAST(DETECTYEAR AS INT64) as detection_year,

        -- Datas de Imagem (Corrigido para BEFOR... conforme schema)
        SAFE_CAST(BEFORIMGDT AS DATE) as image_date_before,
        SAFE_CAST(AFTERIMGDT AS DATE) as image_date_after,
        
        -- Métricas (Originais)
        SAFE_CAST(ALERTHA AS FLOAT64) as alert_area_ha,
        SOURCE as source_satellite,
        BIOME as biome,

        -- Métricas de Sobreposição e Áreas (Novas Colunas)
        ALERTHA as total_alert_ha,
        INLANDHA as overlap_indigenous_ha,
        QUILHA as overlap_quilombola_ha,
        SETTLHA as overlap_settlement_ha,

        -- Classificação e Fontes (Novas Colunas)
        SOURCE as alert_source,
        ALERTCLASS as land_use_class,

        -- Link dinâmico para o laudo oficial (Nova Coluna)
        'https://plataforma.alerta.mapbiomas.org/alerta/' || CAST(ALERTID AS STRING) as mapbiomas_url,
        
        -- Geometria
        ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        
        -- Auditoria
        file_hash,
        ingested_at

    FROM source_data
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY alert_id 
            ORDER BY ingested_at DESC
        ) as row_num
    FROM renamed_and_filtered
)

SELECT * EXCEPT(row_num)
FROM deduplicated
WHERE row_num = 1
  AND detection_date IS NOT NULL
  AND geometry IS NOT NULL
{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='car_code',
    tags=['compliance', 'mapbiomas']
) }}

WITH alerts AS (
    SELECT
        alert_id,
        detection_date,
        detection_year,
        -- Evidência Bitemporal (Perícia)
        image_date_before,
        image_date_after,
        -- Classificação e Prova
        land_use_class,
        mapbiomas_url,
        -- Áreas de sobreposição oficiais (Double Check)
        total_alert_ha,
        overlap_indigenous_ha,
        overlap_quilombola_ha,
        overlap_settlement_ha,
        -- Regra EUDR: Desmatamento após 31/12/2020
        CASE
            WHEN detection_date > '2020-12-31' THEN TRUE
            ELSE FALSE
        END AS is_post_eudr_cutoff
    FROM {{ ref('stg_mapbiomas_alertas') }}
    -- AJUSTE: Incluindo a data 1900-01-01 para que alertas sem data não sejam descartados pelo marco de 2008
    WHERE detection_date >= '2008-07-22' OR detection_date = '1900-01-01'
),

crossings AS (
    SELECT
        alert_id,
        car_code,
        overlap_area_ha
    FROM {{ ref('stg_mapbiomas_property_crossings') }}
)

SELECT
    -- Chaves
    c.car_code,
    c.alert_id,

    -- Temporalidade e Severidade
    CASE 
        WHEN a.detection_date = '1900-01-01' THEN 'Risco Indeterminado'
        ELSE 'Risco Confirmado'
    END AS temporal_risk_category,
    
    a.detection_date,
    a.image_date_before,
    a.image_date_after,
    a.land_use_class,

    -- Métricas de Área (Cruzamento Local vs Oficial)
    c.overlap_area_ha as deforestation_overlap_ha,
    a.total_alert_ha as official_total_alert_ha,

    -- Flags de Risco e Compliance
    a.is_post_eudr_cutoff,

    -- Auditoria de Sobreposições (Double Check)
    a.overlap_indigenous_ha as official_overlap_indigenous_ha,
    a.overlap_quilombola_ha as official_overlap_quilombola_ha,
    a.overlap_settlement_ha as official_overlap_settlement_ha,

    -- Link para Laudo
    a.mapbiomas_url

FROM crossings c
INNER JOIN alerts a ON c.alert_id = a.alert_id
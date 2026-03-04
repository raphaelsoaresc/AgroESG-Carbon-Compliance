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
        -- Apenas para garantir que temos a data
        detection_year
    FROM {{ ref('stg_mapbiomas_alertas') }}
    -- REGRA DO GRANDE JUIZ: Marco Temporal (22 de Julho de 2008)
    WHERE detection_date >= '2008-07-22'
),

crossings AS (
    SELECT 
        alert_id,
        car_code,
        overlap_area_ha
    FROM {{ ref('stg_mapbiomas_property_crossings') }}
)

SELECT
    c.car_code,
    c.alert_id,
    a.detection_date,
    c.overlap_area_ha as deforestation_overlap_ha

FROM crossings c
INNER JOIN alerts a ON c.alert_id = a.alert_id
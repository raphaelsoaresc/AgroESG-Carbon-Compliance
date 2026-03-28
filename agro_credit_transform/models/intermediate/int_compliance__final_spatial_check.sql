{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='car_property_id',
    tags=['compliance']
) }}

WITH car_geometries AS (
    SELECT
        property_id AS car_property_id,
        geometry_raw AS car_geometry,
        geometry_simplified AS car_geometry_simplified,
        ST_BOUNDINGBOX(geometry_simplified) as car_bbox
    FROM {{ ref('int_car_geometries') }}
    WHERE geometry_raw IS NOT NULL
),

dirty_sigef AS (
    SELECT
        slave_labor_tax_id AS tax_id,
        employer_name,
        sigef_property_id,
        SAFE.ST_GEOGFROMTEXT(
            REGEXP_REPLACE(
                REGEXP_REPLACE(sigef_geometry_wkt, r'(\b[A-Z]+)\s+Z\b', r'\1'),
                r'([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?',
                r'\1 \2'
            ),
            make_valid => TRUE
        ) AS sigef_geometry,
        ST_BOUNDINGBOX(SAFE.ST_GEOGFROMTEXT(
            REGEXP_REPLACE(
                REGEXP_REPLACE(sigef_geometry_wkt, r'(\b[A-Z]+)\s+Z\b', r'\1'),
                r'([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?',
                r'\1 \2'
            ),
            make_valid => TRUE
        )) AS sigef_bbox
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    WHERE sigef_geometry_wkt IS NOT NULL
),

dirty_ibama AS (
    SELECT
        tax_id,
        employer_name, 
        detail.is_active_embargo,
        -- 🟢 CORREÇÃO AQUI: A coluna já é geometry, não precisa de ST_GEOGFROMTEXT
        detail.geometry AS ibama_geometry,
        ST_BOUNDINGBOX(detail.geometry) AS ibama_bbox
    FROM {{ ref('int_compliance__identity_check') }},
    UNNEST(ibama_details) AS detail
    WHERE detail.geometry IS NOT NULL
),

dirty_mapbiomas AS (
    SELECT
        car_code,
        alert_id,
        detection_date,
        deforestation_overlap_ha
    FROM {{ ref('int_mapbiomas_deforestation') }}
),

check_slave_labor AS (
    SELECT
        car_property_id,
        MAX(employer_name) AS employer_name,
        MAX(tax_id) AS tax_id,
        'SOCIAL_RISK_SLAVE_LABOR' AS risk_type,
        'OVERLAP WITH SIGEF AREA LINKED TO SLAVE LABOR' AS risk_description,
        SUM(ST_AREA(ST_INTERSECTION(c.car_geometry, s.sigef_geometry)) / 10000) AS overlap_ha
    FROM car_geometries c
    INNER JOIN dirty_sigef s
    ON c.car_bbox.xmin <= s.sigef_bbox.xmax 
       AND c.car_bbox.xmax >= s.sigef_bbox.xmin 
       AND c.car_bbox.ymin <= s.sigef_bbox.ymax 
       AND c.car_bbox.ymax >= s.sigef_bbox.ymin
    WHERE ST_INTERSECTS(c.car_geometry_simplified, s.sigef_geometry)
    GROUP BY car_property_id
),

check_environmental AS (
    SELECT
        car_property_id,
        MAX(employer_name) AS employer_name,
        MAX(tax_id) AS tax_id,
        'ENVIRONMENTAL_RISK_EMBARGO' AS risk_type,
        'OVERLAP WITH IBAMA EMBARGO AREA' AS risk_description,
        SUM(ST_AREA(ST_INTERSECTION(c.car_geometry, i.ibama_geometry)) / 10000) AS overlap_ha
    FROM car_geometries c
    INNER JOIN dirty_ibama i
    ON c.car_bbox.xmin <= i.ibama_bbox.xmax 
       AND c.car_bbox.xmax >= i.ibama_bbox.xmin 
       AND c.car_bbox.ymin <= i.ibama_bbox.ymax 
       AND c.car_bbox.ymax >= i.ibama_bbox.ymin
    WHERE i.is_active_embargo = TRUE
      AND ST_INTERSECTS(c.car_geometry_simplified, i.ibama_geometry)
    GROUP BY car_property_id
),

check_mapbiomas AS (
    SELECT
        m.car_code as car_property_id,
        'MAPBIOMAS ALERT' as employer_name,
        CAST(MAX(m.alert_id) as STRING) as tax_id,
        'ENVIRONMENTAL_RISK_DEFORESTATION' as risk_type,
        CONCAT('CONFIRMED DEFORESTATION AFTER JULY 2008 (Latest Date: ', CAST(MAX(m.detection_date) AS STRING), ')') as risk_description,
        SUM(m.deforestation_overlap_ha) as overlap_ha
    FROM dirty_mapbiomas m
    WHERE m.deforestation_overlap_ha > {{ var('gis_noise_ha_threshold', 0.01) }}
    GROUP BY m.car_code
),

final_unioned AS (
    SELECT * FROM check_slave_labor
    UNION ALL
    SELECT * FROM check_environmental
    UNION ALL
    SELECT * FROM check_mapbiomas
)

SELECT 
    * 
FROM final_unioned
WHERE overlap_ha > {{ var('gis_noise_ha_threshold', 0.01) }}
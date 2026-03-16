{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='car_property_id',
    tags=['compliance']
) }}

WITH car_geometries AS (
    SELECT
        property_id AS car_property_id,
        geometry AS car_geometry
    FROM {{ ref('int_car_geometries') }}
    WHERE geometry IS NOT NULL
),

dirty_sigef AS (
    SELECT
        tax_id,
        employer_name,
        sigef_property_id,
        SAFE.ST_GEOGFROMTEXT(
            REGEXP_REPLACE(
                REGEXP_REPLACE(sigef_geometry_wkt, r'(\b[A-Z]+)\s+Z\b', r'\1'),
                r'([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?',
                r'\1 \2'
            ),
            make_valid => TRUE
        ) AS sigef_geometry
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    WHERE sigef_geometry_wkt IS NOT NULL
),

dirty_ibama AS (
    SELECT
        tax_id,
        employer_name,
        detail.is_active_embargo,
        SAFE.ST_GEOGFROMTEXT(detail.geometry_wkt, make_valid => TRUE) AS ibama_geometry
    FROM {{ ref('int_compliance__identity_check') }},
    UNNEST(ibama_details) AS detail
    WHERE detail.geometry_wkt IS NOT NULL
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
        SUM(ST_AREA(intersection_geom) / 10000) AS overlap_ha
    FROM (
        SELECT
            c.car_property_id,
            s.employer_name,
            s.tax_id,
            ST_INTERSECTION(c.car_geometry, s.sigef_geometry) AS intersection_geom
        FROM car_geometries c
        INNER JOIN dirty_sigef s
        ON ST_INTERSECTS(c.car_geometry, s.sigef_geometry)
    )
    WHERE intersection_geom IS NOT NULL
      AND NOT ST_ISEMPTY(intersection_geom)
      AND ST_AREA(intersection_geom) > 0.0001
    GROUP BY car_property_id
),

check_environmental AS (
    SELECT
        car_property_id,
        MAX(employer_name) AS employer_name,
        MAX(tax_id) AS tax_id,
        'ENVIRONMENTAL_RISK_EMBARGO' AS risk_type,
        'OVERLAP WITH IBAMA EMBARGO AREA' AS risk_description,
        SUM(ST_AREA(intersection_geom) / 10000) AS overlap_ha
    FROM (
        SELECT
            c.car_property_id,
            i.employer_name,
            i.tax_id,
            ST_INTERSECTION(c.car_geometry, i.ibama_geometry) AS intersection_geom
        FROM car_geometries c
        INNER JOIN dirty_ibama i
        ON ST_INTERSECTS(c.car_geometry, i.ibama_geometry)
        WHERE i.is_active_embargo = TRUE
    )
    WHERE intersection_geom IS NOT NULL
      AND NOT ST_ISEMPTY(intersection_geom)
      AND ST_AREA(intersection_geom) > 0.0001
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
    WHERE m.deforestation_overlap_ha > 0.0001
    GROUP BY m.car_code
)

SELECT * FROM check_slave_labor
UNION ALL
SELECT * FROM check_environmental
UNION ALL
SELECT * FROM check_mapbiomas
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

-- Fonte A: Áreas de Trabalho Escravo (via Ponte SIGEF)
dirty_sigef AS (
    SELECT 
        tax_id,
        employer_name,
        sigef_property_id,
        sigef_property_name,
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

-- Fonte B: Áreas de Embargo Ambiental (via CPF/IBAMA)
dirty_ibama AS (
    SELECT 
        tax_id,
        employer_name,
        is_active_embargo,
        SAFE.ST_GEOGFROMTEXT(ibama_geometry_wkt, make_valid => TRUE) AS ibama_geometry
    FROM {{ ref('int_compliance__identity_check') }}
    WHERE ibama_geometry_wkt IS NOT NULL
),

-- Cruzamento 1: CAR vs SIGEF (Trabalho Escravo)
check_slave_labor AS (
    SELECT
        car_property_id,
        employer_name,
        tax_id,
        'SOCIAL_RISK_SLAVE_LABOR' AS risk_type,
        'OVERLAP WITH SIGEF AREA LINKED TO SLAVE LABOR' AS risk_description,
        ST_AREA(intersection_geom) / 10000 AS overlap_ha
    FROM (
        SELECT
            c.car_property_id,
            s.employer_name,
            s.tax_id,
            -- Calculamos a interseção primeiro
            ST_INTERSECTION(c.car_geometry, s.sigef_geometry) AS intersection_geom
        FROM car_geometries c
        INNER JOIN dirty_sigef s
            ON ST_INTERSECTS(c.car_geometry, s.sigef_geometry)
    )
    -- Filtramos geometrias vazias ou nulas antes de passar pelo ST_AREA
    WHERE intersection_geom IS NOT NULL 
        AND NOT ST_ISEMPTY(intersection_geom)
        AND ST_AREA(intersection_geom) > 0.0001
),

-- Cruzamento 2: CAR vs IBAMA (Crime Ambiental)
check_environmental AS (
    SELECT
        car_property_id,
        employer_name,
        tax_id,
        'ENVIRONMENTAL_RISK_EMBARGO' AS risk_type,
        'OVERLAP WITH IBAMA EMBARGO AREA' AS risk_description,
        ST_AREA(intersection_geom) / 10000 AS overlap_ha
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
)

-- Consolidação Final
SELECT * FROM check_slave_labor
UNION ALL
SELECT * FROM check_environmental

-- Verifica se há registos com área zero, geometria nula ou geometrias vazias

WITH car_data AS (
    SELECT 
        'CAR' AS base_origem,
        property_id AS id_propriedade,
        geometry
    FROM {{ ref('int_car_geometries') }}
),

sigef_data AS (
    SELECT 
        'SIGEF' AS base_origem,
        property_id AS id_propriedade,
        geometry
    FROM {{ ref('int_sigef_geometries') }}
),

todas_geometrias AS (
    SELECT * FROM car_data
    UNION ALL
    SELECT * FROM sigef_data
)

SELECT 
    base_origem,
    id_propriedade,
    ST_AREA(geometry) AS area_metros_quadrados
FROM todas_geometrias
WHERE 
    geometry IS NULL 
    OR ST_ISEMPTY(geometry) = TRUE 
    OR ST_AREA(geometry) = 0
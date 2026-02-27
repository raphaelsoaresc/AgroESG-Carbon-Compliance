-- No BigQuery, o retorno da função ST_GEOMETRYTYPE possui o prefixo 'ST_'
-- O perigo real para o nosso motor de compliance é receber Pontos ou Linhas
-- no lugar de polígonos, o que inviabiliza os cálculos de área de Reserva Legal e APPs.

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
    ST_GEOMETRYTYPE(geometry) AS tipo_geometria
FROM todas_geometrias
WHERE 
    geometry IS NOT NULL 
    -- Garante que todas as propriedades são Polígonos ou MultiPolígonos
    AND ST_GEOMETRYTYPE(geometry) NOT IN ('ST_Polygon', 'ST_MultiPolygon')
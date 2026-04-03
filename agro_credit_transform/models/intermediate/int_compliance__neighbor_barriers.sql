{{ config(
    materialized='table',
    cluster_by=['property_id', 'neighbor_id']
) }}

WITH properties AS (
    SELECT property_id, geometry 
    FROM {{ ref('int_car_geometries') }}
    WHERE geometry IS NOT NULL
),

barriers AS (
    -- Usamos o BBOX que você já criou no reference_geometries
    SELECT geometry, bbox 
    FROM {{ ref('int_brazil_reference_geometries') }}
    WHERE restriction_type = 'APP_ZONE' 
      AND restriction_subtype IN ('RIVER', 'WATER_BODY')
),

neighbor_pairs AS (
    SELECT
        f1.property_id,
        f2.property_id as neighbor_id,
        ST_INTERSECTION(f1.geometry, f2.geometry) as intersection_geom,
        -- Criamos o BBOX da interseção na hora para o JOIN ser ultra rápido
        ST_BOUNDINGBOX(ST_INTERSECTION(f1.geometry, f2.geometry)) as intersection_bbox
    FROM properties f1
    INNER JOIN properties f2 ON ST_INTERSECTS(f1.geometry, f2.geometry)
    WHERE f1.property_id < f2.property_id
),

filtered_pairs AS (
    -- Filtro de relevância antes de cruzar com rios
    SELECT * FROM neighbor_pairs
    WHERE ST_LENGTH(intersection_geom) > 5 OR ST_AREA(intersection_geom) > 0.000001
),

barrier_analysis AS (
    SELECT 
        np.property_id,
        np.neighbor_id,
        -- LOGICAL_OR para consolidar se houve barreira
        LOGICAL_OR(ST_DWITHIN(np.intersection_geom, b.geometry, 15)) as has_water_barrier
    FROM filtered_pairs np
    LEFT JOIN barriers b ON 
        -- O PULO DO GATO: Primeiro comparamos os BBOX (que são retângulos simples)
        -- Isso elimina 99.9% dos rios que estão longe sem calcular a distância real
        (np.intersection_bbox.xmin <= b.bbox.xmax AND np.intersection_bbox.xmax >= b.bbox.xmin AND
         np.intersection_bbox.ymin <= b.bbox.ymax AND np.intersection_bbox.ymax >= b.bbox.ymin)
        -- Só o que sobrar passa pelo cálculo pesado
        AND ST_INTERSECTS(np.intersection_geom, ST_BUFFER(b.geometry, 15))
    GROUP BY 1, 2
)

SELECT 
    f.property_id,
    f.neighbor_id,
    f.intersection_geom,
    ST_LENGTH(f.intersection_geom) as shared_boundary_meters,
    ST_AREA(f.intersection_geom) * 10000 as overlap_area_ha,
    COALESCE(b.has_water_barrier, FALSE) as is_physically_blocked
FROM filtered_pairs f
LEFT JOIN barrier_analysis b ON f.property_id = b.property_id AND f.neighbor_id = b.neighbor_id
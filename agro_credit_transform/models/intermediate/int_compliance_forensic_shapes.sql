{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['target_type', 'property_id', 'uf_origem'],
    tags=['compliance', 'forensic']
) }}

WITH 
car_base AS (
    SELECT 
        property_id, 
        uf_origem, 
        geometry_raw as geometry, 
        car_bbox,
        ST_AREA(geometry_raw) / 10000 as property_total_area_ha
    FROM {{ ref('int_car_geometries') }}
),

sigef_base AS (
    SELECT 
        property_id, 
        state_abbreviation as uf_origem, 
        geometry,
        ST_AREA(geometry) / 10000 as property_total_area_ha
    FROM {{ ref('int_sigef_geometries') }}
),

app_zones AS (
    SELECT 
        geometry,
        bbox,
        restriction_subtype 
    FROM {{ ref('int_brazil_reference_geometries') }}
    WHERE restriction_type = 'APP_ZONE'
),

recortes_paredes AS (
    SELECT
        c.property_id, 
        c.uf_origem,
        c.property_total_area_ha,
        CAST(r.restriction_id AS STRING) as target_id,
        CAST(r.restriction_name AS STRING) as target_name,
        ST_INTERSECTION(c.geometry, r.geometry) as geometry,
        CASE 
            WHEN r.restriction_type = 'INDIGENOUS_LAND' THEN 'RECORTE_INVASAO_TI'
            WHEN r.restriction_type = 'QUILOMBOLA' THEN 'RECORTE_INVASAO_QUILOMBO'
            WHEN r.restriction_type = 'CONSERVATION_UNIT' THEN 'RECORTE_INVASAO_UC'
            WHEN r.restriction_type = 'SETTLEMENT' THEN 'RECORTE_INVASAO_ASSENTAMENTO'
            ELSE CONCAT('RECORTE_', r.restriction_type)
        END as target_type
    FROM car_base c
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} r 
        ON  c.car_bbox.xmin <= r.bbox.xmax AND c.car_bbox.xmax >= r.bbox.xmin 
        AND c.car_bbox.ymin <= r.bbox.ymax AND c.car_bbox.ymax >= r.bbox.ymin
        AND ST_INTERSECTS(c.geometry, r.geometry)
    WHERE r.restriction_type NOT IN ('BIOME', 'APP_ZONE', 'INFRASTRUCTURE')
),

-- NOVO RECORTE: INVASÃO DE FAIXA DE DOMÍNIO (RODOVIAS)
recortes_rodovias AS (
    SELECT
        c.property_id, 
        c.uf_origem,
        c.property_total_area_ha,
        CAST(r.restriction_id AS STRING) as target_id,
        CAST(r.restriction_name AS STRING) as target_name,
        ST_INTERSECTION(c.geometry, r.geometry) as geometry,
        'RECORTE_SOBREPOSICAO_RODOVIA' as target_type
    FROM car_base c
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} r 
        ON  c.car_bbox.xmin <= r.bbox.xmax AND c.car_bbox.xmax >= r.bbox.xmin 
        AND c.car_bbox.ymin <= r.bbox.ymax AND c.car_bbox.ymax >= r.bbox.ymin
        AND ST_INTERSECTS(c.geometry, r.geometry)
    WHERE r.restriction_type = 'INFRASTRUCTURE'
      AND r.restriction_subtype = 'ROAD'
),

recortes_embargos AS (
    SELECT
        c.property_id, 
        c.uf_origem,
        c.property_total_area_ha,
        CAST(NULL AS STRING) as target_id,
        CAST(NULL AS STRING) as target_name,
        ST_INTERSECTION(c.geometry, e.geometry) as geometry,
        'RECORTE_EMBARGO' as target_type
    FROM car_base c
    INNER JOIN {{ ref('int_all_embargoes') }} e ON ST_INTERSECTS(c.geometry, e.geometry)
),

recortes_desmatamento AS (
    SELECT 
        c.property_id, 
        c.uf_origem,
        c.property_total_area_ha,
        CAST(a.alert_id AS STRING) as target_id,
        CAST(a.land_use_class AS STRING) as target_name,
        ST_INTERSECTION(c.geometry, a.geometry) as geometry,
        'RECORTE_DESMATAMENTO_MAPBIOMAS' as target_type
    FROM {{ ref('stg_mapbiomas_property_crossings') }} x
    INNER JOIN {{ ref('stg_mapbiomas_alertas') }} a ON x.alert_id = a.alert_id
    INNER JOIN car_base c ON x.car_code = c.property_id
    WHERE a.detection_date >= '{{ var("forest_code_threshold_date", "2008-07-22") }}'
      AND ST_INTERSECTS(c.geometry, a.geometry)
),

recortes_desmatamento_app AS (
    SELECT
        rd.property_id, 
        rd.uf_origem,
        rd.property_total_area_ha,
        rd.target_id,
        rd.target_name,
        ST_INTERSECTION(rd.geometry, app.geometry) as geometry,
        CASE 
            WHEN app.restriction_subtype IN ('RIVER', 'WATER_BODY') THEN 'RECORTE_DESMATAMENTO_EM_APP_HIDRICA'
            ELSE 'RECORTE_DESMATAMENTO_EM_APP'
        END as target_type
    FROM recortes_desmatamento rd
    INNER JOIN app_zones app ON ST_INTERSECTS(rd.geometry, app.geometry)
),

unioned AS (
    SELECT property_id, uf_origem, property_total_area_ha, CAST(NULL AS STRING) as target_id, CAST(NULL AS STRING) as target_name, geometry, 'CAR_TOTAL' as target_type FROM car_base
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, CAST(NULL AS STRING) as target_id, CAST(NULL AS STRING) as target_name, geometry, 'SIGEF_TOTAL' as target_type FROM sigef_base
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, target_id, target_name, geometry, target_type FROM recortes_paredes
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, target_id, target_name, geometry, target_type FROM recortes_rodovias
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, target_id, target_name, geometry, target_type FROM recortes_embargos
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, target_id, target_name, geometry, target_type FROM recortes_desmatamento
    UNION ALL
    SELECT property_id, uf_origem, property_total_area_ha, target_id, target_name, geometry, target_type FROM recortes_desmatamento_app
)

SELECT
    property_id, 
    target_type, 
    target_id,
    target_name,
    uf_origem,
    CASE WHEN uf_origem = 'MT' THEN 'PREMIUM (FEDERAL + ESTADUAL)' ELSE 'STANDARD (FEDERAL)' END as data_source_quality,
    geometry,
    ST_AREA(geometry) / 10000 as target_area_ha,
    property_total_area_ha,
    SAFE_DIVIDE((ST_AREA(geometry) / 10000), property_total_area_ha) * 100 as overlap_pct,
    CURRENT_TIMESTAMP() as processed_at
FROM unioned
WHERE geometry IS NOT NULL 
  AND NOT ST_ISEMPTY(geometry) 
  AND (ST_AREA(geometry) / 10000) > 0.0001
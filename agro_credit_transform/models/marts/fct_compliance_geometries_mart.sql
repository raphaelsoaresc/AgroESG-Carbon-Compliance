-- models/marts/agro_esg_marts/fct_compliance_geometries_mart.sql
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['map_layer', 'property_id'],
    tags=['gold', 'spatial', 'map_service']
) }}

WITH metadata AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        geometry as property_geom,
        property_alias,
        final_eligibility_status,
        is_settlement_identity,
        is_traditional_identity,
        is_quilombo_identity
    FROM {{ ref('fct_compliance_risk') }}
),

infrastructure_evidence AS (
    -- Estradas (Nexo Causal)
    SELECT 
        m.property_id,
        'LOGISTICS' as map_layer, -- Nome corrigido para bater com o config
        'ROAD' as target_type,
        CONCAT('Rodovia: ', r.restriction_name) as info_context,
        '#FFD700' as hex_color, 
        0.8 as fill_opacity,
        2 as z_index,
        ST_INTERSECTION(r.geometry, m.property_geom) as geom
    FROM {{ ref('int_brazil_reference_geometries') }} r
    INNER JOIN metadata m ON ST_INTERSECTS(r.geometry, m.property_geom)
    WHERE r.restriction_subtype = 'ROAD'

    UNION ALL

    -- Linhas de Energia
    SELECT 
        m.property_id,
        'LOGISTICS' as map_layer,
        'POWER_LINE' as target_type,
        CONCAT('Linha de Energia: ', r.restriction_name) as info_context,
        '#FFA500' as hex_color, 
        0.8 as fill_opacity,
        2 as z_index,
        ST_INTERSECTION(r.geometry, m.property_geom) as geom
    FROM {{ ref('int_brazil_reference_geometries') }} r
    INNER JOIN metadata m ON ST_INTERSECTS(r.geometry, m.property_geom)
    WHERE r.restriction_subtype = 'POWER_LINE'
),

forensic_shapes AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        'CRIMINAL' as map_layer,
        target_type,
        CASE 
            WHEN target_type = 'RECORTE_EMBARGO' THEN 'Área de Embargo Ativo'
            WHEN target_type = 'RECORTE_DESMATAMENTO_MAPBIOMAS' THEN 'Desmatamento Detectado'
            WHEN target_type = 'RECORTE_DESMATAMENTO_EUDR' THEN 'Restrição EUDR (Pós-2020)'
            WHEN target_type LIKE 'RECORTE_DESMATAMENTO_EM_APP%' THEN 'Desmatamento em APP'
            ELSE target_type 
        END as info_context,
        CASE 
            WHEN target_type = 'RECORTE_EMBARGO' THEN '#FF0000' 
            WHEN target_type = 'RECORTE_DESMATAMENTO_EUDR' THEN '#8B0000' 
            WHEN target_type LIKE 'RECORTE_DESMATAMENTO_EM_APP%' THEN '#FF4500' 
            ELSE '#DC143C' 
        END as hex_color,
        0.6 as fill_opacity,
        3 as z_index, 
        geometry as geom
    FROM {{ ref('int_compliance_forensic_shapes') }}
    WHERE target_type NOT IN ('CAR_TOTAL', 'SIGEF_TOTAL')
),

shapes_unioned AS (
    -- Limite da Propriedade
    SELECT 
        property_id,
        'BASE' as map_layer,
        'PROPERTY_BOUNDARY' as target_type,
        'Limite do Imóvel Rural' as info_context,
        '#000000' as hex_color, 
        0.1 as fill_opacity,
        1 as z_index, 
        ST_SIMPLIFY(property_geom, 0.0001) as geom
    FROM metadata
    
    UNION ALL
    SELECT property_id, map_layer, target_type, info_context, hex_color, fill_opacity, z_index, ST_SIMPLIFY(geom, 0.0001) FROM forensic_shapes
    UNION ALL
    SELECT property_id, map_layer, target_type, info_context, hex_color, fill_opacity, z_index, ST_SIMPLIFY(geom, 0.0001) FROM infrastructure_evidence
)

SELECT
    s.property_id,
    m.property_alias,
    s.map_layer,
    s.target_type,
    s.info_context,
    s.hex_color,
    s.fill_opacity,
    s.z_index,
    m.final_eligibility_status,
    ST_BOUNDINGBOX(s.geom).xmin as xmin,
    ST_BOUNDINGBOX(s.geom).ymin as ymin,
    ST_BOUNDINGBOX(s.geom).xmax as xmax,
    ST_BOUNDINGBOX(s.geom).ymax as ymax,
    s.geom as geometry,
    CURRENT_TIMESTAMP() as generated_at
FROM shapes_unioned s
INNER JOIN metadata m ON s.property_id = m.property_id
WHERE s.geom IS NOT NULL AND NOT ST_ISEMPTY(s.geom)
  AND NOT (s.target_type = 'RECORTE_INVASAO_ASSENTAMENTO' AND m.is_settlement_identity)
  AND NOT (s.target_type = 'RECORTE_INVASAO_QUILOMBO' AND m.is_quilombo_identity)
  AND NOT (s.target_type = 'RECORTE_INVASAO_TI' AND m.is_traditional_identity)
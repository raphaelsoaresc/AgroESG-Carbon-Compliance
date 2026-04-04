-- models/marts/agro_esg_marts/fct_compliance_geometries_mart.sql
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['map_layer', 'property_id'],
    tags=['gold', 'spatial', 'map_service']
) }}

WITH metadata AS (
    SELECT
        property_id,
        final_eligibility_status,
        uf_origem,
        -- Flags de Identidade Robustas (Sincronizadas com a Fct_Risk)
        is_settlement_identity,
        is_traditional_identity,
        is_quilombo_identity,
        is_ti_identity,    -- Adicionado
        is_uc_identity,    -- Adicionado
        max_adjacency_score
    FROM {{ ref('fct_compliance_risk') }}
),

shapes_unioned AS (
    -- 1. Borda da Propriedade (CAR)
    SELECT
        UPPER(TRIM(property_id)) as property_id,
        'PROPERTY_BOUNDARY' as map_layer,
        'CAR_TOTAL' as target_type,
        ST_SIMPLIFY(geometry_raw, 0.0001) as geom
    FROM {{ ref('int_car_geometries') }}

    UNION ALL

    -- 2. Recortes de Restrição (Desmatamento, Embargo, Invasão)
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        CASE 
            WHEN target_type = 'RECORTE_EMBARGO' THEN 'RESTRICTION_EMBARGO'
            WHEN target_type = 'RECORTE_DESMATAMENTO_MAPBIOMAS' THEN 'RESTRICTION_DEFORESTATION'
            WHEN target_type = 'RECORTE_DESMATAMENTO_EUDR' THEN 'RESTRICTION_EUDR'
            WHEN target_type LIKE 'RECORTE_INVASAO_%' THEN 'RESTRICTION_SOCIAL_ENVIRONMENTAL'
            WHEN target_type LIKE 'RECORTE_DESMATAMENTO_EM_APP%' THEN 'RESTRICTION_APP'
            ELSE 'RESTRICTION_OTHERS'
        END as map_layer,
        target_type,
        ST_SIMPLIFY(geometry, 0.0001) as geom
    FROM {{ ref('int_compliance_forensic_shapes') }}
    WHERE target_type NOT IN ('CAR_TOTAL', 'SIGEF_TOTAL')
)

SELECT
    s.property_id,
    s.map_layer,
    s.target_type,
    m.final_eligibility_status,
    m.uf_origem,
    m.max_adjacency_score,
    -- Bounding Box para Zoom
    (ST_BOUNDINGBOX(s.geom)).xmin as xmin,
    (ST_BOUNDINGBOX(s.geom)).ymin as ymin,
    (ST_BOUNDINGBOX(s.geom)).xmax as xmax,
    (ST_BOUNDINGBOX(s.geom)).ymax as ymax,
    s.geom as geometry,
    CURRENT_TIMESTAMP() as generated_at
FROM shapes_unioned s
INNER JOIN metadata m ON s.property_id = m.property_id
WHERE s.geom IS NOT NULL 
  AND NOT ST_ISEMPTY(s.geom)
  
-- LÓGICA DE COMPLIANCE VISUAL ROBUSTA:
-- Não desenha a mancha de "invasão" se o imóvel for legitimamente daquela categoria
AND NOT (s.target_type = 'RECORTE_INVASAO_ASSENTAMENTO' AND m.is_settlement_identity)
AND NOT (s.target_type = 'RECORTE_INVASAO_QUILOMBO' AND m.is_quilombo_identity)
AND NOT (s.target_type = 'RECORTE_INVASAO_TI' AND m.is_ti_identity) -- Ajustado
AND NOT (s.target_type = 'RECORTE_INVASAO_UC' AND m.is_uc_identity) -- Ajustado
AND NOT (s.target_type = 'RECORTE_TRADITIONAL_TERRITORY' AND m.is_traditional_identity)
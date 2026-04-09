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
        city,
        city_data_source_origin, 
        final_eligibility_status,
        data_reliability_index,
        estimated_financial_liability_brl,
        forensic_summary,
        internal_risks_found,
        uf_origem,
        is_settlement_identity,
        is_traditional_identity,
        is_quilombo_identity,
        max_adjacency_score 
    FROM {{ ref('fct_compliance_risk') }}
),

risky_neighbor_links AS (
    SELECT 
        nb.property_id, 
        nb.neighbor_id, 
        f_neighbor.property_alias as neighbor_alias,
        f_neighbor.final_eligibility_status as neighbor_status,
        f_neighbor.internal_risks_found as neighbor_risks,
        nb.distance_meters,
        nb.connected_road_name
    FROM {{ ref('int_compliance__neighbor_barriers') }} nb
    INNER JOIN {{ ref('fct_compliance_risk') }} f_neighbor 
        ON nb.neighbor_id = f_neighbor.property_id
    WHERE f_neighbor.is_technically_blocked = TRUE 
),

shapes_unioned AS (
    -- 1. Geometria Principal do CAR (Borda da Propriedade)
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        'PROPERTY_BOUNDARY' as map_layer,
        'CAR_TOTAL' as target_type,
        CAST(NULL AS STRING) as info_context,
        ST_SIMPLIFY(geometry_raw, 0.0001) as geom
    FROM {{ ref('int_car_geometries') }}
    
    UNION ALL

    -- 2. Recortes Periciais (Ajustado para granularidade EUDR e APP)
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
        CAST(NULL AS STRING) as info_context,
        ST_SIMPLIFY(geometry, 0.0001) as geom
    FROM {{ ref('int_compliance_forensic_shapes') }}
    WHERE target_type NOT IN ('CAR_TOTAL', 'SIGEF_TOTAL')

    UNION ALL

    -- 3. Geometria dos Vizinhos de Risco (Contexto de Adjacência)
    SELECT 
        UPPER(TRIM(lnk.property_id)) as property_id, 
        'ADJACENT_RISK_SOURCE' as map_layer,
        'NEIGHBOR_BOUNDARY' as target_type,
        CONCAT('Vizinho: ', lnk.neighbor_alias, ' | Status: ', lnk.neighbor_status, ' | Riscos: ', lnk.neighbor_risks) as info_context,
        ST_SIMPLIFY(g.geometry_raw, 0.0001) as geom
    FROM risky_neighbor_links lnk
    INNER JOIN {{ ref('int_car_geometries') }} g ON lnk.neighbor_id = g.property_id
)

SELECT
    s.property_id,
    m.city,
    m.city_data_source_origin, 
    s.map_layer,
    s.target_type,
    s.info_context,
    m.final_eligibility_status,
    m.uf_origem,
    m.data_reliability_index,
    m.estimated_financial_liability_brl,
    m.forensic_summary,
    m.internal_risks_found,
    m.max_adjacency_score,
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
  AND NOT (s.target_type = 'RECORTE_INVASAO_ASSENTAMENTO' AND m.is_settlement_identity)
  AND NOT (s.target_type = 'RECORTE_INVASAO_QUILOMBO' AND m.is_quilombo_identity)
  AND NOT (s.target_type = 'RECORTE_INVASAO_TI' AND m.is_traditional_identity)
  AND NOT (s.target_type = 'RECORTE_INVASAO_TERRA_INDIGENA' AND m.is_traditional_identity)
  AND NOT (s.target_type = 'RECORTE_TRADITIONAL_TERRITORY' AND m.is_traditional_identity)
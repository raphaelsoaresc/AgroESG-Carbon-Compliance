{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['property_id'],
    tags=['gold', 'spatial', 'pericial']
) }}

WITH property_base AS (
    SELECT 
        g.property_id,
        g.geometry_simplified as geom_car_total,
        g.area_ha
    FROM {{ ref('int_car_geometries') }} g
),

-- Agrupamos os recortes por tipo para garantir que cada fazenda tenha apenas UMA linha
infraction_geoms AS (
    SELECT 
        property_id,
        -- Unimos múltiplos recortes do mesmo tipo em um único objeto geográfico (ST_UNION_AGG)
        ST_UNION_AGG(CASE WHEN target_type = 'RECORTE_EMBARGO' THEN geometry_simplified END) as geom_embargos,
        ST_UNION_AGG(CASE WHEN target_type = 'RECORTE_DESMATAMENTO_MAPBIOMAS' THEN geometry_simplified END) as geom_desmatamento,
        ST_UNION_AGG(CASE WHEN target_type IN ('RECORTE_INVASAO_TI', 'RECORTE_INVASAO_UC', 'RECORTE_INVASAO_QUILOMBO') THEN geometry_simplified END) as geom_areas_protegidas,
        ST_UNION_AGG(CASE WHEN target_type = 'RECORTE_CONFLITO_APP' THEN geometry_simplified END) as geom_conflito_app
    FROM (
        -- Subquery para simplificar os recortes antes da união
        SELECT property_id, target_type, ST_SIMPLIFY(geometry, 20) as geometry_simplified 
        FROM {{ ref('int_compliance_forensic_shapes') }}
    )
    GROUP BY 1
),

metadata AS (
    SELECT 
        property_id,
        property_alias,
        final_eligibility_status,
        embargo_area_ha,
        mapbiomas_deforested_ha,
        technical_evidence -- ADICIONADO AQUI
    FROM {{ ref('fct_compliance_risk') }}
)

SELECT
    m.property_id,
    m.property_alias,
    m.final_eligibility_status,
    m.technical_evidence, -- ADICIONADO AQUI
    p.area_ha as area_total_ha,
    
    -- GEOMETRIAS LADO A LADO (Sem duplicação de linhas)
    p.geom_car_total,
    i.geom_embargos,
    i.geom_desmatamento,
    i.geom_areas_protegidas,
    i.geom_conflito_app,

    CURRENT_TIMESTAMP() as generated_at
FROM metadata m
JOIN property_base p ON m.property_id = p.property_id
LEFT JOIN infraction_geoms i ON m.property_id = i.property_id
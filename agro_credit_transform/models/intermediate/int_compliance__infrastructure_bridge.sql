{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id', 'logistics_risk_score'],
    tags=['compliance', 'logistics', 'forensic']
) }}

WITH property_base AS (
    SELECT 
        property_id,
        geometry as property_geom,
        car_bbox
    FROM {{ ref('int_car_geometries') }}
),

infra_base AS (
    SELECT 
        feature_id,
        feature_type,
        feature_subtype,
        evidence_description,
        geometry as infra_geom,
        bbox as infra_bbox
    FROM {{ ref('int_infrastructure_features') }}
),

-- Regra Forense: Pré-calculamos o nexo causal (infra que toca em crimes)
forensic_nexus_list AS (
    SELECT 
        f.property_id,
        i.feature_id,
        TRUE as serves_forensic_violation
    FROM {{ ref('int_compliance_forensic_shapes') }} f
    INNER JOIN {{ ref('int_infrastructure_features') }} i ON ST_INTERSECTS(f.geometry, i.geometry)
    WHERE f.target_type NOT IN ('CAR_TOTAL', 'SIGEF_TOTAL')
    GROUP BY 1, 2
),

spatial_bridge AS (
    SELECT
        p.property_id,
        i.feature_id,
        i.feature_type,
        i.feature_subtype,
        i.evidence_description,
        
        -- Geometria 100% original (sem simplificação)
        ST_DISTANCE(p.property_geom, i.infra_geom) as distance_meters,
        
        CASE 
            WHEN ST_INTERSECTS(p.property_geom, i.infra_geom) THEN 'INTERNAL'
            ELSE 'ADJACENT'
        END as relationship_type,

        -- Regra Forense: Ponto exato de conexão
        ST_CLOSESTPOINT(p.property_geom, i.infra_geom) as entry_point_geom,
        
        -- Regra Forense: Nexo causal com o crime
        COALESCE(fn.serves_forensic_violation, FALSE) as serves_forensic_violation
    FROM property_base p
    INNER JOIN infra_base i ON 
        -- Otimização via BBOX manual (BigQuery Native)
        -- Expandimos a margem em 0.045 graus (~5km) para o join inicial
        p.car_bbox.xmin - 0.045 <= i.infra_bbox.xmax AND p.car_bbox.xmax + 0.045 >= i.infra_bbox.xmin 
        AND p.car_bbox.ymin - 0.045 <= i.infra_bbox.ymax AND p.car_bbox.ymax + 0.045 >= i.infra_bbox.ymin
    LEFT JOIN forensic_nexus_list fn 
        ON p.property_id = fn.property_id AND i.feature_id = fn.feature_id
    WHERE ST_DWITHIN(p.property_geom, i.infra_geom, 5000)
),

scored_evidence AS (
    SELECT
        *,
        -- Regra de Negócio: Pesos definidos para o Score Logístico
        (CASE 
            WHEN feature_subtype = 'AIRSTRIP' AND relationship_type = 'INTERNAL' THEN 50
            WHEN feature_subtype = 'AIRSTRIP' AND relationship_type = 'ADJACENT' THEN 25
            WHEN feature_subtype = 'ROAD' AND relationship_type = 'INTERNAL' THEN 30
            WHEN feature_subtype = 'ROAD' AND relationship_type = 'ADJACENT' THEN 15
            WHEN feature_subtype = 'RIVER' AND relationship_type = 'INTERNAL' THEN 20
            WHEN feature_subtype = 'POWER_LINE' AND relationship_type = 'INTERNAL' THEN 10
            ELSE 5
        END + (CASE WHEN serves_forensic_violation THEN 20 ELSE 0 END)) as feature_risk_score
    FROM spatial_bridge
),

aggregated_evidence AS (
    SELECT
        property_id,
        SUM(feature_risk_score) as total_logistics_score,
        ARRAY_AGG(
            STRUCT(
                feature_id,
                feature_type,
                feature_subtype,
                relationship_type,
                ROUND(distance_meters, 2) as distance_m,
                evidence_description,
                serves_forensic_violation,
                entry_point_geom
            ) ORDER BY feature_risk_score DESC
        ) as logistics_evidence_json
    FROM scored_evidence
    GROUP BY 1
)

SELECT
    a.property_id,
    a.total_logistics_score as logistics_risk_score,
    
    CASE 
        WHEN a.total_logistics_score >= 70 THEN 'CRITICAL'
        WHEN a.total_logistics_score >= 40 THEN 'HIGH'
        WHEN a.total_logistics_score >= 15 THEN 'MEDIUM'
        ELSE 'LOW'
    END as logistics_risk_level,

    a.logistics_evidence_json,
    
    (SELECT STRING_AGG(CONCAT(relationship_type, ': ', feature_subtype), ' | ') 
     FROM UNNEST(a.logistics_evidence_json) LIMIT 3) as top_3_evidence_summary,

    CURRENT_TIMESTAMP() as processed_at
FROM aggregated_evidence a
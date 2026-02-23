{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['eligibility_status', 'biome_name']
) }}

-- 1. DADOS BÁSICOS DA PROPRIEDADE (SIGEF)
WITH sigef AS (
    SELECT 
        property_id,
        property_name,
        geometry,      
        SAFE_DIVIDE(ST_AREA(geometry), 10000) as property_area_ha,
        ingested_at
    FROM {{ ref('int_sigef_geometries') }}
    WHERE property_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) = 1
),

-- 2. RESUMO DAS RESTRIÇÕES UNIFICADAS (BIOMA, FUNAI, INCRA, APP)
reference_summary AS (
    SELECT
        property_id,
        MAX(CASE WHEN restriction_type = 'BIOME' THEN restriction_name END) as biome_name,
        MAX(CASE WHEN restriction_type = 'BIOME' THEN legal_reserve_perc END) as legal_reserve_req,
        LOGICAL_OR(restriction_type = 'INDIGENOUS_LAND') as is_indigenous_land,
        LOGICAL_OR(restriction_type = 'QUILOMBOLA') as is_quilombola_land,
        LOGICAL_OR(restriction_type = 'APP_ZONE') as has_app_area
    FROM {{ ref('int_sigef_reference_matching') }}
    GROUP BY 1
),

-- 3. RESGATE DE BIOMAS (Para os 8 casos "DESCONHECIDO")
-- Fazemos um Cross Join apenas para quem não teve match de bioma
biome_rescue AS (
    SELECT 
        s.property_id,
        ref.restriction_name as rescued_biome_name,
        ref.legal_reserve_perc as rescued_legal_reserve_req
    FROM sigef s
    LEFT JOIN reference_summary r ON s.property_id = r.property_id
    CROSS JOIN {{ ref('int_brazil_reference_geometries') }} ref
    WHERE (r.biome_name IS NULL OR r.biome_name = 'DESCONHECIDO')
      AND ref.restriction_type = 'BIOME'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY s.property_id ORDER BY ST_DISTANCE(s.geometry, ref.geometry) ASC) = 1
),

-- 4. DADOS DE EMBARGO (IBAMA)
embargo_calc AS (
    SELECT
        property_id,
        earliest_embargo_date,
        total_embargo_area_ha
    FROM {{ ref('int_property_embargo_overlap') }}
),

-- 5. UNIÃO DOS CONTEXTOS (Com Coalesce do Resgate)
full_context AS (
    SELECT
        s.property_id,
        s.property_name,
        s.property_area_ha,
        s.geometry,
        -- Prioridade: 1. Match Normal, 2. Resgate, 3. Desconhecido
        UPPER(COALESCE(r.biome_name, br.rescued_biome_name, 'DESCONHECIDO')) as biome_name,
        COALESCE(r.legal_reserve_req, br.rescued_legal_reserve_req, 0.2) as legal_reserve_req,
        
        COALESCE(r.is_indigenous_land, FALSE) as is_indigenous_land,
        COALESCE(r.is_quilombola_land, FALSE) as is_quilombola_land,
        COALESCE(r.has_app_area, FALSE) as has_app_area,
        LEAST(COALESCE(e.total_embargo_area_ha, 0), s.property_area_ha) as embargo_overlap_ha,
        e.earliest_embargo_date as embargo_date
    FROM sigef s
    LEFT JOIN reference_summary r ON s.property_id = r.property_id
    LEFT JOIN biome_rescue br ON s.property_id = br.property_id
    LEFT JOIN embargo_calc e ON s.property_id = e.property_id
),

-- 6. VERDITO DE ELEGIBILIDADE
final_verdict AS (
    SELECT
        *,
        SAFE_DIVIDE(embargo_overlap_ha, property_area_ha) as overlap_percentage,
        CASE 
            WHEN is_indigenous_land THEN 'NOT ELIGIBLE - INDIGENOUS LAND OVERLAP'
            WHEN is_quilombola_land THEN 'NOT ELIGIBLE - QUILOMBOLA TERRITORY OVERLAP'
            WHEN has_app_area AND embargo_overlap_ha > 0.001 THEN 'NOT ELIGIBLE - CRITICAL APP VIOLATION'
            WHEN embargo_overlap_ha > 0.001 AND embargo_date >= '2008-07-22' AND biome_name LIKE 'AMAZ%NIA' 
                THEN 'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION'
            WHEN embargo_overlap_ha > 0.1 AND embargo_date >= '2008-07-22' 
                THEN 'NOT ELIGIBLE - POST-2008 VIOLATION'
            WHEN embargo_overlap_ha > 0.001 AND embargo_date < '2008-07-22' 
                THEN 'ELIGIBLE W/ MONITORING (CONSOLIDATED)'
            WHEN embargo_overlap_ha <= 0.001 THEN 'ELIGIBLE'
            ELSE 'UNDER ANALYSIS'
        END as eligibility_status
    FROM full_context
),

-- 7. REGRA DE CONTAMINAÇÃO
contamination_risk AS (
    SELECT 
        f1.property_id,
        MIN(f2.embargo_date) as neighbor_embargo_date
    FROM (
        SELECT property_id, geometry, ST_GEOHASH(ST_CENTROID(geometry), 5) as geo_prefix 
        FROM final_verdict 
        WHERE eligibility_status LIKE 'ELIGIBLE%'
    ) f1
    INNER JOIN (
        SELECT geometry, embargo_date, ST_GEOHASH(ST_CENTROID(geometry), 5) as geo_prefix
        FROM final_verdict
        WHERE eligibility_status NOT LIKE 'ELIGIBLE%'
    ) f2 ON f1.geo_prefix = f2.geo_prefix 
    WHERE ST_INTERSECTS(f1.geometry, f2.geometry)
    GROUP BY 1
)

-- 8. OUTPUT FINAL
SELECT 
    v.* EXCEPT(embargo_date),
    CASE 
        WHEN c.property_id IS NOT NULL AND v.eligibility_status LIKE 'ELIGIBLE%' 
            THEN 'NOT ELIGIBLE - RISK BY ADJACENCY (CONTAMINATION)'
        ELSE v.eligibility_status 
    END as final_eligibility_status,
    COALESCE(v.embargo_date, c.neighbor_embargo_date) as final_embargo_date,
    ST_Y(ST_CENTROID(v.geometry)) as latitude,
    ST_X(ST_CENTROID(v.geometry)) as longitude,
    ST_ASGEOJSON(ST_SIMPLIFY(v.geometry, 10)) as geom_json, 
    CONCAT('Propriedade ', CAST(ABS(MOD(FARM_FINGERPRINT(CAST(v.property_id AS STRING)), 100000000000)) AS STRING)) as property_alias
FROM final_verdict v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY property_area_ha DESC) = 1
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['eligibility_status', 'biome_name']
) }}

-- 1. DADOS BÁSICOS DA PROPRIEDADE
WITH sigef AS (
    SELECT 
        property_id,
        property_name,
        geometry,      
        geom_calc,     
        SAFE_DIVIDE(ST_AREA(geometry), 10000) as property_area_ha
    FROM {{ ref('int_sigef_geometries') }}
    WHERE property_id IS NOT NULL -- <--- ADICIONE ESTA LINHA AQUI
),

-- 2. UNIÃO DOS CONTEXTOS (BIOMA + EMBARGO)
full_context AS (
    SELECT
        s.property_id,
        s.property_name,
        s.property_area_ha,
        s.geometry,
        COALESCE(b.biome_name, 'DESCONHECIDO') as biome_name,
        COALESCE(b.legal_reserve_perc, 0.2) as legal_reserve_req,
        COALESCE(e.total_embargo_area_ha, 0) as embargo_overlap_ha,
        e.earliest_embargo_date as embargo_date,
        CASE WHEN e.property_id IS NOT NULL THEN TRUE ELSE FALSE END as has_embargo
    FROM sigef s
    LEFT JOIN {{ ref('int_property_biome_classification') }} b ON s.property_id = b.property_id
    LEFT JOIN {{ ref('int_property_embargo_overlap') }} e ON s.property_id = e.property_id
),

-- 3. VERDITO INDIVIDUAL (LÓGICA DE NEGÓCIO)
final_verdict AS (
    SELECT
        *,
        SAFE_DIVIDE(embargo_overlap_ha, property_area_ha) as overlap_percentage,
        CASE 
            WHEN embargo_overlap_ha < 0.001 THEN 'ELIGIBLE'
            WHEN embargo_overlap_ha < 0.1 THEN 'ELIGIBLE - NEGLIGIBLE OVERLAP'
            WHEN embargo_date >= '2008-07-22' THEN 
                CASE 
                    WHEN biome_name = 'AMAZÔNIA' THEN 'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION'
                    ELSE 'NOT ELIGIBLE - POST-2008 VIOLATION'
                END
            WHEN embargo_date < '2008-07-22' THEN 'ELIGIBLE W/ MONITORING (CONSOLIDATED)'
            ELSE 'UNDER ANALYSIS'
        END as eligibility_status
    FROM full_context
),

-- 4. REGRA DE CONTAMINAÇÃO (RISCO POR VIZINHANÇA)
contamination_risk AS (
    SELECT DISTINCT
        f1.property_id
    FROM (
        SELECT property_id, geometry, ST_GEOHASH(ST_CENTROID(geometry), 5) as geo_prefix 
        FROM final_verdict 
        WHERE eligibility_status LIKE 'ELIGIBLE%'
    ) f1
    INNER JOIN (
        SELECT geometry, ST_GEOHASH(ST_CENTROID(geometry), 5) as geo_prefix
        FROM final_verdict
        WHERE eligibility_status LIKE 'NOT ELIGIBLE%'
    ) f2 ON f1.geo_prefix = f2.geo_prefix 
    
    WHERE ST_INTERSECTS(f1.geometry, f2.geometry)
),

-- 5. APLICAÇÃO DA PREVALÊNCIA FINAL
final_with_contamination AS (
    SELECT 
        v.*,
        CASE 
            WHEN c.property_id IS NOT NULL THEN 'NOT ELIGIBLE - RISK BY OVERLAP (CONTAMINATION)'
            ELSE v.eligibility_status 
        END as final_eligibility_status
    FROM final_verdict v
    LEFT JOIN contamination_risk c ON v.property_id = c.property_id
)

-- 6. OUTPUT FINAL COM ALIAS
SELECT 
    * EXCEPT(property_id, eligibility_status, final_eligibility_status),
    final_eligibility_status as eligibility_status,
    CONCAT('Propriedade ', CAST(ABS(FARM_FINGERPRINT(CAST(property_id AS STRING))) AS STRING)) as property_alias
FROM final_with_contamination
QUALIFY ROW_NUMBER() OVER(PARTITION BY property_alias ORDER BY property_area_ha DESC) = 1

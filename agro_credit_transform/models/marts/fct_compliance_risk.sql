{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['final_eligibility_status', 'biome_name'],
    tags=['car', 'mart']
) }}

-- 1. DADOS BÁSICOS E GEOMETRIA (CAR)
WITH properties AS (
    SELECT 
        property_id,
        CONCAT('Imóvel CAR ', SUBSTR(property_id, 0, 10), '...') as property_name,
        centroid,
        geometry,      
        area_ha as property_area_ha,
        ingested_at
    FROM {{ ref('int_car_geometries') }}
),

-- 2. METRICAS DE COMPLIANCE (Reserva Legal e APP)
compliance_metrics AS (
    SELECT
        property_id,
        biome_name,
        total_rl_declared_ha,
        required_rl_ha,
        rl_balance_ha,
        rl_status,
        total_app_declared_ha
    FROM {{ ref('int_car_compliance_metrics') }}
),

-- 3. RESTRIÇÕES ESPACIAIS CRÍTICAS (Hard Blocks)
hard_blocks AS (
    SELECT
        property_id,
        TRUE as has_hard_block_overlap,
        total_overlap_ha as protected_area_overlap_ha,
        overlaps_details
    FROM {{ ref('int_car_spatial_restrictions') }}
),

-- 4. CRUZAMENTO COM EMBARGOS (IBAMA)
embargo_check AS (
    SELECT
        p.property_id,
        MIN(i.embargo_date) as earliest_embargo_date,
        SUM(ST_AREA(ST_INTERSECTION(p.geometry, i.geometry)) / 10000) as embargo_area_ha
    FROM properties p
    INNER JOIN {{ ref('int_ibama_geometries') }} i
        ON ST_INTERSECTS(p.geometry, i.geometry)
    WHERE ST_INTERSECTSBOX(i.geometry, -61.7, -18.1, -50.1, -7.3)
    GROUP BY 1
),

-- 5. CONTEXTO UNIFICADO
full_context AS (
    SELECT
        p.property_id,
        p.property_name,
        p.property_area_ha,
        p.geometry,
        p.centroid,
        
        COALESCE(c.biome_name, 'DESCONHECIDO') as biome_name,
        
        c.rl_status,
        c.rl_balance_ha,
        
        COALESCE(hb.has_hard_block_overlap, FALSE) as is_protected_area_overlap,
        COALESCE(hb.protected_area_overlap_ha, 0) as protected_overlap_ha,
        
        COALESCE(e.embargo_area_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date

    FROM properties p
    LEFT JOIN compliance_metrics c ON p.property_id = c.property_id
    LEFT JOIN hard_blocks hb ON p.property_id = hb.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
),

-- 6. VERDITO DE ELEGIBILIDADE
final_verdict AS (
    SELECT
        *,
        CASE 
            -- 0. REGRA ANTI-FRAUDE (Inconsistência de Dados)
            WHEN embargo_area_ha > (property_area_ha + 0.5)
                THEN 'NOT ELIGIBLE - DATA INCONSISTENCY (EMBARGO > AREA)'

            -- 1. Bloqueio Total: Invasão de TI/Quilombola/UC
            WHEN is_protected_area_overlap AND protected_overlap_ha > 0.1 
                THEN 'NOT ELIGIBLE - PROTECTED AREA INVASION'
            
            -- 2. Bloqueio Total: Embargo Pós-2008 (Acima da tolerância técnica de 0.1ha)
            WHEN embargo_area_ha > 0.1 AND embargo_date >= '2008-07-22' 
                THEN 'NOT ELIGIBLE - IBAMA EMBARGO (POST-2008)'

            -- 2.5. Alerta de Micro-Embargo (Entre 0.001ha e 0.1ha)
            -- Resolve o problema das 46 fazendas com sobreposição mínima (erro de GPS)
            WHEN embargo_area_ha > 0.001 AND embargo_date >= '2008-07-22'
                THEN 'WARNING - MICRO EMBARGO (REVIEW REQUIRED)'
            
            -- 3. Alerta Grave: Embargo Antigo
            WHEN embargo_area_ha > 0.1 AND embargo_date < '2008-07-22' 
                THEN 'WARNING - PRE-2008 EMBARGO (MONITORING REQUIRED)'
            
            -- 4. Alerta Ambiental: Déficit de Reserva Legal
            WHEN rl_status = 'DEFICIT' 
                THEN 'CONDITIONAL - LEGAL RESERVE DEFICIT (REQUIRES PRA)'
            
            ELSE 'ELIGIBLE'
        END as eligibility_status
    FROM full_context
),

-- 7. RISCO DE CONTAMINAÇÃO
contamination_risk AS (
    SELECT 
        f1.property_id,
        TRUE as has_contaminated_neighbor
    FROM (
        SELECT property_id, geometry, ST_GEOHASH(centroid, 5) as geo_prefix 
        FROM final_verdict 
        WHERE eligibility_status LIKE 'ELIGIBLE%' OR eligibility_status LIKE 'CONDITIONAL%'
    ) f1
    INNER JOIN (
        SELECT geometry, ST_GEOHASH(centroid, 5) as geo_prefix
        FROM final_verdict
        WHERE eligibility_status LIKE 'NOT ELIGIBLE%'
    ) f2 ON f1.geo_prefix = f2.geo_prefix 
    WHERE ST_INTERSECTS(f1.geometry, f2.geometry)
    GROUP BY 1
)

-- 8. OUTPUT FINAL
SELECT 
    v.property_id,
    v.property_name,
    v.biome_name,
    v.property_area_ha,
    
    CASE 
        WHEN c.has_contaminated_neighbor AND v.eligibility_status LIKE 'ELIGIBLE%' 
            THEN 'WARNING - RISK BY ADJACENCY'
        ELSE v.eligibility_status 
    END as final_eligibility_status,
    
    v.rl_status,
    ROUND(v.rl_balance_ha, 2) as rl_balance_ha,
    v.embargo_date,
    ROUND(v.embargo_area_ha, 2) as embargo_area_ha,
    
    -- Colunas necessárias para os testes passarem
    v.is_protected_area_overlap, 
    v.protected_overlap_ha,

    ST_Y(v.centroid) as latitude,
    ST_X(v.centroid) as longitude,
    ST_ASGEOJSON(ST_SIMPLIFY(v.geometry, 10)) as geom_json,
    
    -- Alias robusto (12 chars Hex) para evitar colisão no teste unique
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
    
    CURRENT_TIMESTAMP() as mart_updated_at

FROM final_verdict v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
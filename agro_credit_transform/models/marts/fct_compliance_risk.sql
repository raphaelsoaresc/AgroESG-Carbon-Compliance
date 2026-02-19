{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['eligibility_status', 'biome_name']
) }}

-- 1. DADOS BÁSICOS DA PROPRIEDADE (Com Deduplicação)
WITH sigef AS (
    SELECT 
        property_id,
        property_name,
        geometry,      
        SAFE_DIVIDE(ST_AREA(geometry), 10000) as property_area_ha
    FROM {{ ref('int_sigef_geometries') }}
    WHERE property_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) = 1
),

-- 2. PREPARAÇÃO DO IBAMA
ibama_raw AS (
    SELECT geometry, embargo_date 
    FROM {{ ref('int_ibama_geometries') }}
    WHERE is_active_embargo = TRUE AND geometry IS NOT NULL
),

-- 3. CÁLCULO DE EMBARGO (ST_UNION_AGG para evitar dupla contagem)
embargo_calc AS (
    SELECT
        s.property_id,
        MIN(i.embargo_date) as earliest_embargo_date,
        ST_AREA(
            ST_INTERSECTION(ANY_VALUE(s.geometry), ST_UNION_AGG(i.geometry))
        ) / 10000 as raw_embargo_area_ha
    FROM sigef s
    JOIN ibama_raw i ON ST_INTERSECTS(s.geometry, i.geometry)
    GROUP BY 1
),

-- 4. UNIÃO DOS CONTEXTOS
full_context AS (
    SELECT
        s.property_id,
        s.property_name,
        s.property_area_ha,
        s.geometry,
        UPPER(COALESCE(b.biome_name, 'DESCONHECIDO')) as biome_name,
        COALESCE(b.legal_reserve_perc, 0.2) as legal_reserve_req,
        LEAST(
            COALESCE(e.raw_embargo_area_ha, 0),
            s.property_area_ha
        ) as embargo_overlap_ha,
        e.earliest_embargo_date as embargo_date,
        CASE WHEN e.property_id IS NOT NULL THEN TRUE ELSE FALSE END as has_embargo
    FROM sigef s
    LEFT JOIN {{ ref('int_property_biome_classification') }} b ON s.property_id = b.property_id
    LEFT JOIN embargo_calc e ON s.property_id = e.property_id
),

-- 5. VERDITO INDIVIDUAL
final_verdict AS (
    SELECT
        *,
        SAFE_DIVIDE(embargo_overlap_ha, property_area_ha) as overlap_percentage,
        CASE 
            WHEN embargo_overlap_ha < 0.001 THEN 'ELIGIBLE'
            WHEN embargo_date >= '2008-07-22' AND biome_name LIKE 'AMAZ%NIA' THEN 'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION'
            WHEN embargo_overlap_ha < 0.1 THEN 'ELIGIBLE - NEGLIGIBLE OVERLAP'
            WHEN embargo_date >= '2008-07-22' THEN 'NOT ELIGIBLE - POST-2008 VIOLATION'
            WHEN embargo_date < '2008-07-22' THEN 'ELIGIBLE W/ MONITORING (CONSOLIDATED)'
            ELSE 'UNDER ANALYSIS'
        END as eligibility_status
    FROM full_context
),

-- 6. REGRA DE CONTAMINAÇÃO (Herança de data do vizinho)
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
        WHERE eligibility_status LIKE 'NOT ELIGIBLE%'
    ) f2 ON f1.geo_prefix = f2.geo_prefix 
    
    WHERE ST_INTERSECTS(f1.geometry, f2.geometry)
    GROUP BY 1
),

-- 7. APLICAÇÃO DA PREVALÊNCIA FINAL
final_with_contamination AS (
    SELECT 
        v.* EXCEPT(embargo_date),
        CASE 
            WHEN c.property_id IS NOT NULL THEN 'NOT ELIGIBLE - RISK BY OVERLAP (CONTAMINATION)'
            ELSE v.eligibility_status 
        END as final_eligibility_status,
        COALESCE(v.embargo_date, c.neighbor_embargo_date) as final_embargo_date
    FROM final_verdict v
    LEFT JOIN contamination_risk c ON v.property_id = c.property_id
) -- <--- AQUI NÃO PODE TER VÍRGULA

-- 8. OUTPUT FINAL (Sem a vírgula antes e com todas as colunas)
SELECT 
    * EXCEPT(property_id, eligibility_status, final_eligibility_status, final_embargo_date),
    
    -- Colunas necessárias para o Parquet / Streamlit
    property_id,
    
    CASE 
        WHEN final_eligibility_status = 'ELIGIBLE' THEN NULL
        ELSE CAST(final_embargo_date AS STRING) 
    END as embargo_date,

    ST_Y(ST_CENTROID(geometry)) as latitude,
    ST_X(ST_CENTROID(geometry)) as longitude,
    ST_ASGEOJSON(ST_SIMPLIFY(geometry, 10)) as geom_json, 
    
    final_eligibility_status as eligibility_status,
    CONCAT('Propriedade ', CAST(ABS(MOD(FARM_FINGERPRINT(CAST(property_id AS STRING)), 100000000000)) AS STRING)) as property_alias

FROM final_with_contamination
QUALIFY ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY property_area_ha DESC) = 1
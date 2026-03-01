{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['final_eligibility_status', 'biome_name'],
    tags=['compliance', 'esg', 'satellite']
) }}

{% set forest_code_date = var('forest_code_threshold_date', '2008-07-22') %}
{% set gis_noise_ha = var('gis_noise_ha_threshold', 0.1) %}
{% set gis_noise_pct = var('gis_noise_pct_threshold', 0.005) %}
{% set min_lon = var('bbox_min_lon', -74.0) %}
{% set min_lat = var('bbox_min_lat', -34.0) %}
{% set max_lon = var('bbox_max_lon', -34.0) %}
{% set max_lat = var('bbox_max_lat', 5.0) %}

WITH properties AS (
    SELECT 
        property_id, 
        area_ha as property_area_ha, 
        geometry, 
        centroid
    FROM {{ ref('int_car_geometries') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY area_ha DESC) = 1
),

compliance_metrics AS (
    SELECT property_id, biome_name, rl_status, rl_balance_ha 
    FROM {{ ref('int_car_compliance_metrics') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY rl_balance_ha DESC) = 1
),

hard_blocks AS (
    SELECT 
        property_id, 
        TRUE as has_hard_block_overlap, 
        SUM(total_overlap_ha) as protected_area_overlap_ha 
    FROM {{ ref('int_car_spatial_restrictions') }}
    GROUP BY 1
),

embargo_check AS (
    SELECT 
        p.property_id, 
        MIN(i.embargo_date) as earliest_embargo_date, 
        SUM(ST_AREA(ST_INTERSECTION(p.geometry, i.geometry)) / 10000) as embargo_area_ha
    FROM properties p 
    INNER JOIN {{ ref('int_ibama_geometries') }} i ON ST_INTERSECTS(p.geometry, i.geometry)
    -- ADICIONE AQUI A OTIMIZAÇÃO:
    WHERE ST_INTERSECTSBOX(i.geometry, {{ min_lon }}, {{ min_lat }}, {{ max_lon }}, {{ max_lat }})
    GROUP BY 1
),

slave_labor_check AS (
    SELECT 
        car_property_id, 
        MAX(overlap_ha) as slave_labor_overlap_ha, 
        MAX(employer_name) as slave_labor_offender
    FROM {{ ref('int_compliance__final_spatial_check') }} 
    WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR' 
    GROUP BY 1
),

satellite_check AS (
    SELECT 
        property_id, 
        is_high_slope_risk, 
        vegetation_state, 
        is_critical_environmental_violation as is_satellite_violation,
        max_slope_degrees, 
        ndvi_mean 
    FROM {{ ref('stg_property_topography') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY processed_at DESC) = 1
),

full_context AS (
    SELECT
        p.*, 
        COALESCE(c.biome_name, 'DESCONHECIDO') as biome_name, 
        c.rl_status, 
        c.rl_balance_ha,
        COALESCE(hb.has_hard_block_overlap, FALSE) as is_protected_area_overlap,
        COALESCE(hb.protected_area_overlap_ha, 0) as protected_overlap_ha,
        COALESCE(e.embargo_area_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date,
        COALESCE(sl.slave_labor_overlap_ha, 0) as slave_labor_overlap_ha,
        sl.slave_labor_offender,
        COALESCE(s.is_high_slope_risk, FALSE) as is_high_slope_risk,
        COALESCE(s.is_satellite_violation, FALSE) as is_satellite_violation,
        COALESCE(s.vegetation_state, 'NO_DATA') as vegetation_state,
        s.max_slope_degrees, 
        s.ndvi_mean
    FROM properties p
    LEFT JOIN compliance_metrics c ON p.property_id = c.property_id
    LEFT JOIN hard_blocks hb ON p.property_id = hb.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
    LEFT JOIN slave_labor_check sl ON p.property_id = sl.car_property_id
    LEFT JOIN satellite_check s ON p.property_id = s.property_id
),

final_verdict AS (
    SELECT
        *,
        CASE 
            -- 1. BLOQUEIOS HARD (Prioridade Máxima)
            WHEN slave_labor_overlap_ha > 0 
                THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
            
            WHEN is_satellite_violation 
                THEN 'NOT ELIGIBLE - SATELLITE ALERT (DEFORESTATION DETECTED)'
            
            WHEN is_protected_area_overlap AND (protected_overlap_ha > {{ gis_noise_ha }} OR (protected_overlap_ha / property_area_ha) > {{ gis_noise_pct }}) 
                THEN 'NOT ELIGIBLE - PROTECTED AREA INVASION'
            
            WHEN (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha > 0.001 AND embargo_date >= '{{ forest_code_date }}')
                 OR (embargo_area_ha > {{ gis_noise_ha }} AND embargo_date >= '{{ forest_code_date }}')
                THEN 'NOT ELIGIBLE - IBAMA EMBARGO (POST-2008)'

            -- 2. ALERTAS DE DADOS (Nuvens)
            WHEN vegetation_state = 'CLOUD_COVERED' 
                THEN 'INFO - AWAITING SATELLITE CLEARANCE (HIGH CLOUDS)'
            
            -- 3. BLOQUEIOS SOFT / CONDICIONAIS
            WHEN embargo_area_ha > {{ gis_noise_ha }} AND (embargo_date < '{{ forest_code_date }}' OR embargo_date IS NULL) 
                THEN 'WARNING - PRE-2008 OR UNDATED EMBARGO (MONITORING REQUIRED)'
            
            WHEN rl_status = 'DEFICIT' 
                THEN 'CONDITIONAL - LEGAL RESERVE DEFICIT (PRA REQUIRED)'
            
            ELSE 'ELIGIBLE'
        END as base_eligibility_status
    FROM full_context
),

contamination_risk AS (
    -- Melhoria: Join espacial puro para bater com o teste assert_adjacency
    SELECT DISTINCT
        f1.property_id
    FROM final_verdict f1
    INNER JOIN final_verdict f2 ON ST_INTERSECTS(f1.geometry, f2.geometry)
    WHERE f1.base_eligibility_status IN ('ELIGIBLE', 'INFO - AWAITING SATELLITE CLEARANCE (HIGH CLOUDS)', 'CONDITIONAL - LEGAL RESERVE DEFICIT (PRA REQUIRED)')
      AND f2.base_eligibility_status LIKE 'NOT ELIGIBLE%'
      AND f1.property_id != f2.property_id
)

SELECT 
    v.property_id,
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
    v.biome_name,
    v.property_area_ha,
    
    -- Se houver risco de contaminação, ele "sobrescreve" Eligible, Info e Conditional
    CASE 
        WHEN c.property_id IS NOT NULL THEN 'WARNING - RISK BY ADJACENCY'
        ELSE v.base_eligibility_status 
    END as final_eligibility_status,

    v.slave_labor_offender,
    v.slave_labor_overlap_ha,
    v.embargo_date,
    v.embargo_area_ha,
    v.is_satellite_violation,
    v.vegetation_state,
    v.max_slope_degrees,
    v.ndvi_mean,
    ST_Y(v.centroid) as latitude,
    ST_X(v.centroid) as longitude,
    ST_ASGEOJSON(ST_SIMPLIFY(v.geometry, 5)) as geom_json,
    CURRENT_TIMESTAMP() as mart_updated_at,
    v.rl_status,
    v.rl_balance_ha,
    v.is_protected_area_overlap,
    v.protected_overlap_ha,
    v.is_high_slope_risk

FROM final_verdict v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY v.property_id ORDER BY v.property_area_ha DESC) = 1
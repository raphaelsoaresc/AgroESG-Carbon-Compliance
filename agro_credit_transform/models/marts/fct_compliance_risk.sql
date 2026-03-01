{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['final_eligibility_status', 'biome_name'],
    tags=['compliance', 'esg', 'satellite']
) }}

WITH properties AS (
    SELECT property_id, area_ha as property_area_ha, geometry, centroid FROM {{ ref('int_car_geometries') }}
),

compliance_metrics AS (
    SELECT property_id, biome_name, rl_status, rl_balance_ha FROM {{ ref('int_car_compliance_metrics') }}
),

hard_blocks AS (
    SELECT property_id, TRUE as has_hard_block_overlap, total_overlap_ha as protected_area_overlap_ha FROM {{ ref('int_car_spatial_restrictions') }}
),

embargo_check AS (
    SELECT 
        p.property_id, MIN(i.embargo_date) as earliest_embargo_date, 
        SUM(ST_AREA(ST_INTERSECTION(p.geometry, i.geometry)) / 10000) as embargo_area_ha
    FROM properties p 
    INNER JOIN {{ ref('int_ibama_geometries') }} i ON ST_INTERSECTS(p.geometry, i.geometry)
    WHERE ST_INTERSECTSBOX(i.geometry, -74.0, -34.0, -34.0, 5.0) 
    GROUP BY 1
),

slave_labor_check AS (
    SELECT car_property_id, MAX(overlap_ha) as slave_labor_overlap_ha, MAX(employer_name) as slave_labor_offender
    FROM {{ ref('int_compliance__final_spatial_check') }} WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR' GROUP BY 1
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
),

full_context AS (
    SELECT
        p.*, COALESCE(c.biome_name, 'DESCONHECIDO') as biome_name, c.rl_status, c.rl_balance_ha,
        COALESCE(hb.has_hard_block_overlap, FALSE) as is_protected_area_overlap,
        COALESCE(hb.protected_area_overlap_ha, 0) as protected_overlap_ha,
        COALESCE(e.embargo_area_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date,
        COALESCE(sl.slave_labor_overlap_ha, 0) as slave_labor_overlap_ha,
        sl.slave_labor_offender,
        COALESCE(s.is_high_slope_risk, FALSE) as is_high_slope_risk,
        COALESCE(s.is_satellite_violation, FALSE) as is_satellite_violation,
        COALESCE(s.vegetation_state, 'NO_DATA') as vegetation_state,
        s.max_slope_degrees, s.ndvi_mean
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
            WHEN slave_labor_overlap_ha > 0 THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
            WHEN is_satellite_violation THEN 'NOT ELIGIBLE - SATELLITE ALERT (DEFORESTATION ON HIGH SLOPE)'
            WHEN is_protected_area_overlap AND protected_overlap_ha > 0.1 THEN 'NOT ELIGIBLE - PROTECTED AREA INVASION'
            WHEN embargo_area_ha > 0.1 AND embargo_date >= '2008-07-22' THEN 'NOT ELIGIBLE - IBAMA EMBARGO (POST-2008)'
            
            WHEN is_high_slope_risk AND vegetation_state = 'CLOUD_COVERED' THEN 'INFO - AWAITING SATELLITE CLEARANCE (HIGH CLOUDS)'
            
            WHEN embargo_area_ha > 0.1 AND embargo_date < '2008-07-22' THEN 'WARNING - PRE-2008 EMBARGO (MONITORING REQUIRED)'
            WHEN rl_status = 'DEFICIT' THEN 'CONDITIONAL - LEGAL RESERVE DEFICIT'
            ELSE 'ELIGIBLE'
        END as eligibility_status
    FROM full_context
),

-- REGRA DE ADJACÊNCIA: O "Pulo do Gato" final
contamination_risk AS (
    SELECT 
        f1.property_id,
        TRUE as has_contaminated_neighbor
    FROM final_verdict f1
    INNER JOIN final_verdict f2 ON ST_INTERSECTS(f1.geometry, f2.geometry)
    WHERE f1.eligibility_status IN ('ELIGIBLE', 'CONDITIONAL - LEGAL RESERVE DEFICIT')
        AND f2.eligibility_status LIKE 'NOT ELIGIBLE%'
        AND f1.property_id != f2.property_id
    GROUP BY 1
)

SELECT 
    v.property_id,
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 8)) as property_alias,
    v.biome_name,
    v.property_area_ha,
    
    -- Aplicação do Risco de Adjacência no Status Final
    CASE 
        WHEN c.has_contaminated_neighbor AND v.eligibility_status = 'ELIGIBLE' 
        THEN 'WARNING - RISK BY ADJACENCY'
        ELSE v.eligibility_status 
    END as final_eligibility_status,

    v.slave_labor_offender,
    v.embargo_date,
    v.is_satellite_violation,
    v.vegetation_state,
    v.max_slope_degrees,
    v.ndvi_mean,
    ST_Y(v.centroid) as latitude,
    ST_X(v.centroid) as longitude,
    ST_ASGEOJSON(ST_SIMPLIFY(v.geometry, 5)) as geom_json,
    CURRENT_TIMESTAMP() as mart_updated_at
FROM final_verdict v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['final_eligibility_status', 'biome_name'],
    tags=['compliance', 'legal', 'cmn_5081']
) }}

{% set forest_code_date = var('forest_code_threshold_date', '2008-07-22') %}
{% set gis_noise_ha = var('gis_noise_ha_threshold', 0.1) %}

WITH properties AS (
    SELECT 
        UPPER(TRIM(g.property_id)) as property_id, 
        g.area_ha, 
        TRIM(o.registration_status) as registration_status, 
        g.geometry, 
        g.centroid
    FROM {{ ref('int_car_geometries') }} g
    LEFT JOIN {{ ref('stg_car_owners') }} o ON UPPER(TRIM(g.property_id)) = UPPER(TRIM(o.property_id))
),

sicar_native_overlaps AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        LOGICAL_OR(overlap_source IN ('FUNAI', 'TI', 'TERRA_INDIGENA')) as sicar_flag_ti,
        LOGICAL_OR(overlap_source IN ('INCRA', 'QUILOMBOLA')) as sicar_flag_quilombola,
        LOGICAL_OR(overlap_source IN ('ICMBIO', 'ESTADUAL', 'UC', 'UNIDADE_CONSERVACAO')) as sicar_flag_uc,
        SUM(overlap_area_ha) as sicar_total_overlap_ha
    FROM {{ ref('stg_car_overlaps') }}
    GROUP BY 1
),

spatial_restrictions AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        EXISTS(SELECT 1 FROM UNNEST(overlaps_details) x WHERE x.restriction_type = 'INDIGENOUS_LAND') as gis_flag_ti,
        EXISTS(SELECT 1 FROM UNNEST(overlaps_details) x WHERE x.restriction_type = 'QUILOMBOLA') as gis_flag_quilombola,
        EXISTS(SELECT 1 FROM UNNEST(overlaps_details) x WHERE x.restriction_type = 'CONSERVATION_UNIT') as gis_flag_uc,
        total_overlap_ha as gis_total_overlap_ha
    FROM {{ ref('int_car_spatial_restrictions') }}
),

slave_labor_combined AS (
    -- União do Match Territorial (Bridge) e Match Espacial (Final Spatial Check)
    SELECT 
        UPPER(TRIM(sigef_property_id)) as property_id,
        MAX(territorial_match_confidence) as match_confidence
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    WHERE territorial_match_confidence IN ('HIGH', 'MEDIUM')
    GROUP BY 1
    
    UNION DISTINCT
    
    SELECT 
        UPPER(TRIM(car_property_id)) as property_id,
        'HIGH' as match_confidence
    FROM {{ ref('int_compliance__final_spatial_check') }}
    WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
),

embargo_check AS (
    SELECT 
        UPPER(TRIM(p.property_id)) as property_id, 
        MIN(i.embargo_date) as earliest_embargo_date, 
        SUM(ST_AREA(ST_INTERSECTION(p.geometry, i.geometry)) / 10000) as embargo_area_ha
    FROM properties p 
    INNER JOIN {{ ref('int_ibama_geometries') }} i ON ST_INTERSECTS(p.geometry, i.geometry)
    GROUP BY 1
),

satellite_data AS (
    SELECT UPPER(TRIM(property_id)) as property_id, * EXCEPT(property_id) 
    FROM {{ ref('int_satellite_metrics_union') }}
),

full_context AS (
    SELECT
        p.*,
        COALESCE(sn.sicar_flag_ti OR sr.gis_flag_ti, FALSE) as is_ti_overlap,
        COALESCE(sn.sicar_flag_quilombola OR sr.gis_flag_quilombola, FALSE) as is_quilombola_overlap,
        COALESCE(sn.sicar_flag_uc OR sr.gis_flag_uc, FALSE) as is_uc_overlap,
        GREATEST(COALESCE(sn.sicar_total_overlap_ha, 0), COALESCE(sr.gis_total_overlap_ha, 0)) as total_protected_overlap_ha,
        COALESCE(sl.match_confidence, 'NONE') as slave_labor_match,
        COALESCE(e.embargo_area_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date,
        sat.max_slope_degrees,
        sat.general_ndvi_mean,
        sat.app_ndvi_mean,
        sat.is_app_violation_risk,
        sat.alert_selective_deforestation_in_app,
        sat.app_vegetation_status,
        sat.general_vegetation_state,
        c.biome_name,
        c.rl_status
    FROM properties p
    LEFT JOIN sicar_native_overlaps sn ON p.property_id = sn.property_id
    LEFT JOIN spatial_restrictions sr ON p.property_id = sr.property_id
    LEFT JOIN slave_labor_combined sl ON p.property_id = sl.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
    LEFT JOIN satellite_data sat ON p.property_id = sat.property_id
    LEFT JOIN {{ ref('int_car_compliance_metrics') }} c ON p.property_id = c.property_id
),

final_analysis AS (
    SELECT
        *,
        CASE 
            WHEN slave_labor_match != 'NONE' THEN 'NOT ELIGIBLE - SOCIAL'
            WHEN is_ti_overlap OR is_quilombola_overlap THEN 'NOT ELIGIBLE - TRADITIONAL TERRITORY'
            WHEN registration_status IN ('CANCELADO', 'SUSPENSO') THEN 'NOT ELIGIBLE - CAR STATUS'
            WHEN (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha > 0) THEN 'NOT ELIGIBLE - IBAMA AMAZON (CMN 5.081)'
            WHEN (embargo_area_ha > {{ gis_noise_ha }} AND embargo_date >= '{{ forest_code_date }}') THEN 'NOT ELIGIBLE - IBAMA'
            WHEN is_uc_overlap AND total_protected_overlap_ha > {{ gis_noise_ha }} THEN 'NOT ELIGIBLE - CONSERVATION UNIT'
            WHEN max_slope_degrees > 45 OR is_app_violation_risk OR alert_selective_deforestation_in_app THEN 'NOT ELIGIBLE - SATELLITE'
            WHEN app_vegetation_status = 'CLOUD_COVERED' OR general_vegetation_state = 'CLOUD_COVERED' THEN 'AWAITING CLEARANCE'
            WHEN embargo_area_ha > {{ gis_noise_ha }} AND (embargo_date < '{{ forest_code_date }}' OR embargo_date IS NULL) THEN 'WARNING - OLD EMBARGO'
            WHEN rl_status = 'DEFICIT' THEN 'CONDITIONAL - RL DEFICIT'
            ELSE 'ELIGIBLE'
        END as base_eligibility_status
    FROM full_context
),

contamination_risk AS (
    -- Identifica propriedades ELIGIBLE que tocam em propriedades NOT ELIGIBLE
    SELECT DISTINCT f1.property_id
    FROM final_analysis f1
    INNER JOIN final_analysis f2 ON ST_INTERSECTS(f1.geometry, f2.geometry)
    WHERE f1.base_eligibility_status IN ('ELIGIBLE', 'AWAITING CLEARANCE', 'CONDITIONAL - RL DEFICIT')
      AND f2.base_eligibility_status LIKE 'NOT ELIGIBLE%'
      AND f1.property_id != f2.property_id
)

SELECT 
    v.property_id,
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
    v.area_ha,
    v.area_ha as property_area_ha,
    COALESCE(v.registration_status, 'ATIVO') as car_status,
    v.biome_name,
    
    -- Status Final com Adjacência
    CASE 
        WHEN c.property_id IS NOT NULL THEN 'WARNING - RISK BY ADJACENCY'
        ELSE v.base_eligibility_status 
    END as final_eligibility_status,

    -- Colunas exigidas pelos testes customizados (Nomes Exatos)
    (v.is_ti_overlap OR v.is_quilombola_overlap OR v.is_uc_overlap) as is_protected_area_overlap,
    v.total_protected_overlap_ha as protected_overlap_ha,
    v.total_protected_overlap_ha as protected_area_overlap_ha,
    v.embargo_area_ha,
    v.embargo_date,

    -- Evidência Técnica
    CASE 
        WHEN c.property_id IS NOT NULL THEN "Propriedade adjacente a imóvel com restrições socioambientais críticas."
        WHEN v.slave_labor_match != 'NONE' THEN "Identificado risco de trabalho escravo (Match Territorial ou Espacial)."
        WHEN v.registration_status IN ('CANCELADO', 'SUSPENSO') THEN FORMAT("Imóvel com CAR %s no SICAR.", v.registration_status)
        WHEN v.is_ti_overlap OR v.is_quilombola_overlap THEN "Sobreposição com Terra Indígena ou Quilombola detectada."
        WHEN v.biome_name LIKE 'AMAZ%NIA' AND v.embargo_area_ha > 0 THEN FORMAT("Bloqueio CMN 5.081: Embargo na Amazônia (%.4f ha).", v.embargo_area_ha)
        WHEN v.is_app_violation_risk THEN FORMAT("Degradação de APP detectada via satélite (NDVI: %.2f).", v.app_ndvi_mean)
        WHEN v.max_slope_degrees > 45 THEN FORMAT("APP por declividade (%.2f°).", v.max_slope_degrees)
        ELSE "Propriedade em conformidade com os critérios analisados."
    END as technical_evidence,

    v.max_slope_degrees,
    v.general_ndvi_mean,
    v.app_ndvi_mean,
    CURRENT_TIMESTAMP() as analyzed_at

FROM final_analysis v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
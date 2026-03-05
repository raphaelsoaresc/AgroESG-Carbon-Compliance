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
        -- 1. MUNICÍPIO: Adicionado aqui na fonte
        
        g.city, 
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
    SELECT 
        property_id,
        MAX(match_confidence) as match_confidence,
        ARRAY_AGG(DISTINCT employer_name IGNORE NULLS) as employers
    FROM (
        SELECT UPPER(TRIM(sigef_property_id)) as property_id, territorial_match_confidence as match_confidence, employer_name FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }} WHERE territorial_match_confidence IN ('HIGH', 'MEDIUM')
        UNION ALL
        SELECT UPPER(TRIM(car_property_id)) as property_id, 'HIGH' as match_confidence, employer_name FROM {{ ref('int_compliance__final_spatial_check') }} WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
    ) GROUP BY 1
),

embargo_check AS (
    SELECT 
        UPPER(TRIM(p.property_id)) as property_id, 
        COALESCE(MIN(i.embargo_date), CAST('1900-01-01' AS DATE)) as earliest_embargo_date, 
        SUM(ST_AREA(ST_INTERSECTION(p.geometry, i.geometry)) / 10000) as embargo_area_ha
    FROM properties p 
    INNER JOIN {{ ref('int_ibama_geometries') }} i ON ST_INTERSECTS(p.geometry, i.geometry)
    GROUP BY 1
),

-- 3. MAPBIOMAS: Lógica mantida (já estava correta para pegar o ID)
mapbiomas_check AS (
    SELECT
        UPPER(TRIM(car_code)) as property_id,
        MAX(detection_date) as latest_deforestation_date,
        SUM(deforestation_overlap_ha) as total_deforested_ha,
        -- Pega o ID do alerta mais recente como evidência
        ARRAY_AGG(alert_id ORDER BY detection_date DESC LIMIT 1)[OFFSET(0)] as latest_alert_id
    FROM {{ ref('int_mapbiomas_deforestation') }}
    GROUP BY 1
),

satellite_data AS (
    SELECT UPPER(TRIM(property_id)) as property_id, * EXCEPT(property_id) FROM {{ ref('int_satellite_metrics_union') }}
),

full_context AS (
    SELECT
        p.*,
        COALESCE(sn.sicar_flag_ti OR sr.gis_flag_ti, FALSE) as is_ti_overlap,
        COALESCE(sn.sicar_flag_quilombola OR sr.gis_flag_quilombola, FALSE) as is_quilombola_overlap,
        COALESCE(sn.sicar_flag_uc OR sr.gis_flag_uc, FALSE) as is_uc_overlap,
        GREATEST(COALESCE(sn.sicar_total_overlap_ha, 0), COALESCE(sr.gis_total_overlap_ha, 0)) as total_protected_overlap_ha,
        COALESCE(sl.match_confidence, 'NONE') as slave_labor_match,
        sl.employers as slave_labor_employers,
        COALESCE(e.embargo_area_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date,
        
        -- MapBiomas Data
        COALESCE(mb.total_deforested_ha, 0) as mapbiomas_deforested_ha,
        mb.latest_deforestation_date as mapbiomas_date,
        mb.latest_alert_id as mapbiomas_alert_id,

        sat.max_slope_degrees, sat.general_ndvi_mean, sat.app_ndvi_mean,
        sat.is_app_violation_risk, sat.alert_selective_deforestation_in_app,
        sat.app_vegetation_status, sat.general_vegetation_state,
        c.biome_name, 
        c.rl_status,
        -- 2. RL DEFICIT: Trazendo os valores numéricos
        c.rl_deficit_ha,
        c.rl_balance_ha
    FROM properties p
    LEFT JOIN sicar_native_overlaps sn ON p.property_id = sn.property_id
    LEFT JOIN spatial_restrictions sr ON p.property_id = sr.property_id
    LEFT JOIN slave_labor_combined sl ON p.property_id = sl.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
    LEFT JOIN mapbiomas_check mb ON p.property_id = mb.property_id 
    LEFT JOIN satellite_data sat ON p.property_id = sat.property_id
    LEFT JOIN {{ ref('int_car_compliance_metrics') }} c ON p.property_id = c.property_id
),

final_analysis AS (
    SELECT
        *,
        -- internal_risks_found: Tags para status e adjacência
        ARRAY_TO_STRING(ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN slave_labor_match != 'NONE' THEN 'SOCIAL' END,
                CASE WHEN is_ti_overlap OR is_quilombola_overlap THEN 'TRADITIONAL_TERRITORY' END,
                CASE WHEN registration_status IN ('CANCELADO', 'SUSPENSO') THEN 'CAR_STATUS' END,
                CASE WHEN (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha > 0) THEN 'CMN_5081' END,
                CASE WHEN (embargo_area_ha > {{ gis_noise_ha }} AND embargo_date >= '{{ forest_code_date }}') THEN 'IBAMA' END,
                
                -- REGRA DO GRANDE JUIZ: Se tem desmatamento MapBiomas > ruído, é bloqueio
                CASE WHEN mapbiomas_deforested_ha > {{ gis_noise_ha }} THEN 'MAPBIOMAS' END,

                CASE WHEN (embargo_area_ha > {{ gis_noise_ha }} AND (embargo_date < '{{ forest_code_date }}' OR embargo_date IS NULL)) THEN 'OLD_EMBARGO' END,
                CASE WHEN is_uc_overlap AND total_protected_overlap_ha > {{ gis_noise_ha }} THEN 'CONSERVATION_UNIT' END,
                CASE WHEN is_app_violation_risk OR alert_selective_deforestation_in_app OR max_slope_degrees > 48 THEN 'SATELLITE' END,
                CASE WHEN max_slope_degrees > 45 AND max_slope_degrees <= 48 THEN 'BORDERLINE_SLOPE' END,
                CASE WHEN app_vegetation_status = 'CLOUD_COVERED' THEN 'CLOUD_COVER' END,
                CASE WHEN rl_status = 'DEFICIT' THEN 'RL_DEFICIT' END
            ]) AS x WHERE x IS NOT NULL
        ), ' | ') as internal_risks_found,

        ARRAY_TO_STRING(ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN slave_labor_match != 'NONE' THEN 'Trabalho Escravo' END,
                CASE WHEN (embargo_area_ha > {{ gis_noise_ha }} AND embargo_date >= '{{ forest_code_date }}') THEN FORMAT('Embargo IBAMA (Bloqueio): %.2f ha', embargo_area_ha) END,
                
                -- EVIDÊNCIA MAPBIOMAS (Com ID)
                CASE WHEN mapbiomas_deforested_ha > {{ gis_noise_ha }} THEN FORMAT('Desmatamento MapBiomas (Alerta %d): %.2f ha em %t', mapbiomas_alert_id, mapbiomas_deforested_ha, mapbiomas_date) END,

                CASE WHEN (embargo_area_ha > {{ gis_noise_ha }} AND (embargo_date < '{{ forest_code_date }}' OR embargo_date IS NULL)) THEN FORMAT('Embargo Histórico (Aviso): %.2f ha', embargo_area_ha) END,
                CASE WHEN total_protected_overlap_ha > {{ gis_noise_ha }} THEN 'Sobreposição em Área Protegida' END,
                CASE WHEN app_vegetation_status = 'CLOUD_COVERED' THEN 'Análise de satélite obstruída por nuvens' END,
                
                -- 2. RL DEFICIT: Evidência melhorada com valor numérico
                CASE WHEN rl_status = 'DEFICIT' THEN FORMAT('Déficit de RL: %.2f ha', rl_deficit_ha) END,
                
                CASE WHEN max_slope_degrees > 45 AND max_slope_degrees <= 48 THEN FORMAT('Alerta de Declividade: %.2f°', max_slope_degrees) END
            ]) AS x WHERE x IS NOT NULL
        ), ' | ') as detailed_evidence_string,

        ARRAY_TO_STRING(ARRAY(
            SELECT x FROM UNNEST([
                FORMAT('Declividade: %.1f°', max_slope_degrees),
                FORMAT('NDVI: %.2f', general_ndvi_mean),
                FORMAT('Bioma: %s', biome_name)
            ]) AS x
        ), ' | ') as compliance_metrics_string
    FROM full_context
),

contamination_risk AS (
    SELECT 
        f1.property_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT f2.internal_risks_found), ' e ') as neighbor_statuses
    FROM final_analysis f1
    INNER JOIN final_analysis f2 ON ST_INTERSECTS(f1.geometry, f2.geometry)
    WHERE f1.property_id != f2.property_id
      AND (
          f2.internal_risks_found LIKE '%SOCIAL%' 
          OR f2.internal_risks_found LIKE '%TRADITIONAL_TERRITORY%' 
          OR f2.internal_risks_found LIKE '%CAR_STATUS%'
          OR f2.internal_risks_found LIKE '%CMN_5081%'
          OR f2.internal_risks_found LIKE '%IBAMA%' 
          OR f2.internal_risks_found LIKE '%MAPBIOMAS%' 
          OR f2.internal_risks_found LIKE '%CONSERVATION_UNIT%'
          OR f2.internal_risks_found LIKE '%SATELLITE%'
      )
    GROUP BY 1
)

SELECT 
    v.property_id,
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
    v.area_ha as property_area_ha,
    v.area_ha,
    -- 1. MUNICÍPIO: Exposto no final
    v.city,
    COALESCE(v.registration_status, 'ATIVO') as car_status,
    v.biome_name,
    
    CASE 
        WHEN v.internal_risks_found LIKE '%SOCIAL%' THEN 'NOT ELIGIBLE - SOCIAL'
        WHEN v.internal_risks_found LIKE '%TRADITIONAL_TERRITORY%' THEN 'NOT ELIGIBLE - TRADITIONAL TERRITORY'
        WHEN v.internal_risks_found LIKE '%CAR_STATUS%' THEN 'NOT ELIGIBLE - CAR STATUS'
        WHEN v.internal_risks_found LIKE '%CMN_5081%' THEN 'NOT ELIGIBLE - IBAMA AMAZON (CMN 5.081)'
        WHEN v.internal_risks_found LIKE '%IBAMA%' THEN 'NOT ELIGIBLE - IBAMA'
        WHEN v.internal_risks_found LIKE '%MAPBIOMAS%' THEN 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)'
        WHEN v.internal_risks_found LIKE '%CONSERVATION_UNIT%' THEN 'NOT ELIGIBLE - CONSERVATION UNIT'
        WHEN v.internal_risks_found LIKE '%SATELLITE%' THEN 'NOT ELIGIBLE - SATELLITE'
        WHEN c.property_id IS NOT NULL THEN 'WARNING - RISK BY ADJACENCY'
        WHEN v.internal_risks_found LIKE '%OLD_EMBARGO%' THEN 'WARNING - OLD EMBARGO'
        WHEN v.internal_risks_found LIKE '%BORDERLINE_SLOPE%' THEN 'WARNING - BORDERLINE SLOPE'
        WHEN v.internal_risks_found LIKE '%CLOUD_COVER%' THEN 'AWAITING CLEARANCE'
        WHEN v.internal_risks_found LIKE '%RL_DEFICIT%' THEN 'CONDITIONAL - RL DEFICIT'
        ELSE 'ELIGIBLE'
    END as final_eligibility_status,

    CONCAT(
        CASE WHEN v.detailed_evidence_string != '' THEN CONCAT("⚠️ RESTRIÇÕES: ", v.detailed_evidence_string, " | ") ELSE "✅ EM CONFORMIDADE | " END,
        CASE WHEN c.property_id IS NOT NULL THEN CONCAT("🚧 VIZINHANÇA: Vizinho com [", c.neighbor_statuses, "] | ") ELSE "" END,
        CONCAT("📊 MÉTRICAS: ", v.compliance_metrics_string)
    ) as technical_evidence,

    v.internal_risks_found,
    c.neighbor_statuses as adjacency_details,
    ARRAY_TO_STRING(v.slave_labor_employers, ' | ') as slave_labor_offender,
    ST_Y(v.centroid) as latitude,
    ST_X(v.centroid) as longitude,
    v.max_slope_degrees,
    v.general_ndvi_mean,
    v.app_ndvi_mean,
    v.embargo_area_ha,
    v.embargo_date,
    
    -- 3. MAPBIOMAS: Métricas e ID na saída final
    v.mapbiomas_deforested_ha,
    v.mapbiomas_date,
    v.mapbiomas_alert_id,

    -- 2. RL DEFICIT: Dados brutos na saída final
    v.rl_deficit_ha,
    v.rl_balance_ha,

    v.total_protected_overlap_ha as protected_area_overlap_ha,
    v.total_protected_overlap_ha as protected_overlap_ha,
    (v.is_ti_overlap OR v.is_quilombola_overlap OR v.is_uc_overlap) as is_protected_area_overlap,
    CURRENT_TIMESTAMP() as analyzed_at

FROM final_analysis v
LEFT JOIN contamination_risk c ON v.property_id = c.property_id
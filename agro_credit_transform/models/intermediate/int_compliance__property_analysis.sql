-- models/intermediate/int_compliance__property_analysis.sql
{{ config(
    materialized='table',
    cluster_by=['property_id', 'uf_origem']
) }}

WITH legal_params AS (
    SELECT
        MAX(CASE WHEN parameter_name = 'fine_deforestation_per_ha' THEN CAST(value AS FLOAT64) END) as fine_defo,
        MAX(CASE WHEN parameter_name = 'fine_protected_area_per_ha' THEN CAST(value AS FLOAT64) END) as fine_protected,
        MAX(CASE WHEN parameter_name = 'fine_slave_labor_fixed' THEN CAST(value AS FLOAT64) END) as fine_slave,
        MAX(CASE WHEN parameter_name = 'fine_app_violation_fixed' THEN CAST(value AS FLOAT64) END) as fine_app,
        MAX(CASE WHEN parameter_name = 'fine_rl_deficit_per_ha' THEN CAST(value AS FLOAT64) END) as fine_rl,
        MAX(CASE WHEN parameter_name = 'fine_embargo_per_ha' THEN CAST(value AS FLOAT64) END) as fine_embargo,
        MAX(CASE WHEN parameter_name = 'gis_noise_ha_threshold' THEN CAST(value AS FLOAT64) END) as noise_threshold,
        MAX(CASE WHEN parameter_name = 'eudr_cutoff_date' THEN CAST(value AS DATE) END) as eudr_date,
        MAX(CASE WHEN parameter_name = 'forest_code_cutoff_date' THEN CAST(value AS DATE) END) as forest_code_date
    FROM {{ ref('legal_parameters') }}
),

properties AS (
    SELECT
        UPPER(TRIM(p_meta.property_id)) as property_id,
        p_meta.property_type,
        COALESCE(p_class.final_area_ha, 0) as area_ha,
        p_meta.city,
        UPPER(TRIM(p_meta.uf_origem)) as uf_origem,
        -- PADRONIZAÇÃO DE STATUS (Siglas do Governo)
        CASE 
            WHEN UPPER(TRIM(o.registration_status)) IN ('AT', 'ATIVO', 'ANALISADO', 'CCT', 'ANALISADO (COMPLETO)') THEN 'ATIVO'
            WHEN UPPER(TRIM(o.registration_status)) IN ('PE', 'PENDENTE') THEN 'PENDENTE'
            WHEN UPPER(TRIM(o.registration_status)) IN ('SU', 'SUSPENSO') THEN 'SUSPENSO'
            WHEN UPPER(TRIM(o.registration_status)) IN ('CA', 'CANCELADO') THEN 'CANCELADO'
            ELSE COALESCE(UPPER(TRIM(o.registration_status)), 'ATIVO')
        END as registration_status,
        g.geometry_raw as geometry,
        g.centroid,
        g.car_bbox,
        CASE WHEN g.geometry_raw IS NULL THEN TRUE ELSE FALSE END as is_missing_geometry,
        p_class.final_fiscal_modules as fiscal_modules,
        p_class.producer_size_category,
        p_class.is_small_holder,
        p_class.fmp_ha
    FROM {{ ref('stg_car_properties') }} p_meta
    LEFT JOIN {{ ref('int_car_geometries') }} g ON UPPER(TRIM(p_meta.property_id)) = UPPER(TRIM(g.property_id))
    LEFT JOIN {{ ref('stg_car_owners') }} o ON UPPER(TRIM(p_meta.property_id)) = UPPER(TRIM(o.property_id))
    LEFT JOIN {{ ref('int_car_properties_classified') }} p_class ON UPPER(TRIM(p_meta.property_id)) = p_class.property_id
),

forensic_areas AS (
    SELECT
        UPPER(TRIM(property_id)) as property_id,
        SUM(CASE WHEN target_type = 'RECORTE_DESMATAMENTO_MAPBIOMAS' THEN target_area_ha ELSE 0 END) as forensic_defo_ha,
        SUM(CASE WHEN target_type = 'RECORTE_DESMATAMENTO_EM_APP' THEN target_area_ha ELSE 0 END) as app_deforested_ha_forensic,
        SUM(CASE WHEN target_type = 'RECORTE_DESMATAMENTO_EM_APP_HIDRICA' THEN target_area_ha ELSE 0 END) as forensic_app_hidrica_ha,
        SUM(CASE WHEN target_type = 'RECORTE_DESMATAMENTO_EM_APP_DECLIVIDADE' THEN target_area_ha ELSE 0 END) as forensic_app_declividade_ha,
        SUM(CASE WHEN target_type = 'RECORTE_EMBARGO' THEN target_area_ha ELSE 0 END) as forensic_embargo_ha,
        SUM(CASE WHEN target_type = 'RECORTE_INVASAO_TI' THEN target_area_ha ELSE 0 END) as forensic_ti_ha,
        SUM(CASE WHEN target_type = 'RECORTE_INVASAO_QUILOMBO' THEN target_area_ha ELSE 0 END) as forensic_quilombo_ha,
        SUM(CASE WHEN target_type = 'RECORTE_INVASAO_UC' THEN target_area_ha ELSE 0 END) as forensic_uc_ha,
        SUM(CASE WHEN target_type = 'RECORTE_INVASAO_ASSENTAMENTO' THEN target_area_ha ELSE 0 END) as forensic_settlement_ha,
        SUM(CASE WHEN target_type = 'RECORTE_TRADITIONAL_TERRITORY' THEN target_area_ha ELSE 0 END) as forensic_traditional_ha,
        MAX(CASE WHEN target_type = 'RECORTE_INVASAO_ASSENTAMENTO' THEN overlap_pct ELSE 0 END) as settlement_overlap_pct,
        MAX(CASE WHEN target_type = 'RECORTE_TRADITIONAL_TERRITORY' THEN overlap_pct ELSE 0 END) as traditional_overlap_pct,
        ANY_VALUE(CASE WHEN target_type = 'RECORTE_INVASAO_ASSENTAMENTO' THEN target_name END) as settlement_name,
        ANY_VALUE(CASE WHEN target_type = 'RECORTE_TRADITIONAL_TERRITORY' THEN target_name END) as traditional_name,
        ANY_VALUE(CASE WHEN target_type = 'RECORTE_INVASAO_TI' THEN target_name END) as ti_name,
        ANY_VALUE(CASE WHEN target_type = 'RECORTE_INVASAO_UC' THEN target_name END) as uc_name,
        ANY_VALUE(CASE WHEN target_type = 'RECORTE_INVASAO_QUILOMBO' THEN target_name END) as quilombo_name,
        MAX(data_source_quality) as data_source_quality
    FROM {{ ref('int_compliance_forensic_shapes') }}
    GROUP BY 1
),

slave_labor_combined AS (
    SELECT
        s.property_id,
        MAX(s.territorial_match_confidence) as match_confidence,
        ARRAY_AGG(DISTINCT s.employer_name IGNORE NULLS) as employers,
        MAX(mte.inclusion_date) as slave_labor_inclusion_date
    FROM (
        SELECT UPPER(TRIM(sigef_property_id)) as property_id, territorial_match_confidence, employer_name FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
        UNION ALL
        SELECT UPPER(TRIM(car_property_id)) as property_id, 'HIGH' as territorial_match_confidence, employer_name FROM {{ ref('int_compliance__final_spatial_check') }} WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
    ) s
    LEFT JOIN {{ ref('stg_mte_slave_labor') }} mte ON UPPER(TRIM(s.employer_name)) = UPPER(TRIM(mte.employer_name))
    GROUP BY 1
),

embargo_check AS (
    SELECT
        UPPER(TRIM(property_id)) as property_id,
        earliest_embargo_date,
        has_any_active_embargo,
        has_any_cancelled_embargo,
        embargo_offenders,
        embargo_processes,
        embargo_tax_ids,
        embargo_reported_areas,
        embargo_sources
    FROM {{ ref('int_property_embargo_overlap') }}
),

mapbiomas_check AS (
    SELECT
        UPPER(TRIM(car_code)) as property_id,
        MAX(detection_date) as latest_deforestation_date,
        MIN(image_date_before) as earliest_evidence_date,
        MAX(image_date_after) as latest_evidence_date,
        -- Agregamos os IDs e URLs em strings para não duplicar linhas
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(alert_id AS STRING) IGNORE NULLS), ' | ') as mapbiomas_alert_ids,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT land_use_class IGNORE NULLS), ' | ') as mapbiomas_classes,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT mapbiomas_url IGNORE NULLS), ' | ') as mapbiomas_report_links,
        -- Somamos as áreas oficiais para comparação
        SUM(COALESCE(deforestation_overlap_ha, 0)) as mapbiomas_total_overlap_ha,
        SUM(COALESCE(official_total_alert_ha, 0)) as mapbiomas_official_alert_ha,
        -- Flags de sobreposição oficial
        SUM(COALESCE(official_overlap_indigenous_ha, 0)) as mapbiomas_official_ti_ha,
        SUM(COALESCE(official_overlap_quilombola_ha, 0)) as mapbiomas_official_quilombo_ha,
        SUM(COALESCE(official_overlap_settlement_ha, 0)) as mapbiomas_official_settlement_ha,
        -- Verificamos se algum dos alertas é pós-EUDR
        LOGICAL_OR(is_post_eudr_cutoff) as has_official_eudr_alert
    FROM {{ ref('int_mapbiomas_deforestation') }}
    GROUP BY 1
),

full_context AS (
    SELECT
        p.*, 
        lp.*, -- GARANTE QUE AS MULTAS (fine_defo, etc) ESTEJAM DISPONÍVEIS
        sat.max_slope_degrees, sat.relief_classification, sat.last_update,
        COALESCE(f.forensic_embargo_ha, 0) as embargo_area_ha_raw,
        e.earliest_embargo_date as embargo_date,
        COALESCE(e.has_any_active_embargo, FALSE) as has_any_active_embargo,
        COALESCE(e.has_any_cancelled_embargo, FALSE) as has_any_cancelled_embargo,
        e.embargo_offenders, e.embargo_processes, e.embargo_tax_ids, e.embargo_reported_areas, e.embargo_sources,
        COALESCE(f.forensic_defo_ha, 0) as mapbiomas_deforested_ha_raw,
        COALESCE(f.app_deforested_ha_forensic, 0) as app_deforested_ha_forensic,
        f.forensic_app_hidrica_ha, f.forensic_app_declividade_ha,
        mb.latest_deforestation_date as mapbiomas_date,
        mb.earliest_evidence_date, mb.latest_evidence_date,
        mb.mapbiomas_classes, mb.mapbiomas_report_links,
        mb.mapbiomas_alert_ids,
        mb.mapbiomas_total_overlap_ha,
        mb.mapbiomas_official_alert_ha,
        mb.mapbiomas_official_ti_ha,
        mb.mapbiomas_official_quilombo_ha,
        mb.mapbiomas_official_settlement_ha,
        mb.has_official_eudr_alert,
        CASE WHEN (mb.latest_deforestation_date >= lp.eudr_date OR mb.latest_deforestation_date IS NULL) 
             THEN COALESCE(f.forensic_defo_ha, 0) ELSE 0 END as eudr_deforested_ha,
        sl.match_confidence as slave_labor_match_confidence,
        sl.slave_labor_inclusion_date,
        COALESCE(c.biome_name, 'N/A') as biome_name, 
        c.rl_status, c.rl_deficit_ha, c.rl_balance_ha,
        (COALESCE(f.forensic_ti_ha, 0) + COALESCE(f.forensic_uc_ha, 0) + COALESCE(f.forensic_quilombo_ha, 0) + COALESCE(f.forensic_settlement_ha, 0) + COALESCE(f.forensic_traditional_ha, 0)) as protected_area_overlap_ha_raw,
        f.forensic_ti_ha, f.forensic_quilombo_ha, f.forensic_uc_ha, f.forensic_settlement_ha, f.forensic_traditional_ha, f.data_source_quality,
        COALESCE(p.property_type = 'AST' OR f.settlement_overlap_pct > 90, FALSE) as is_settlement_identity,
        COALESCE(p.property_type = 'PCT' OR f.traditional_overlap_pct > 90, FALSE) as is_traditional_identity,
        COALESCE(p.property_type = 'PCT' AND f.forensic_quilombo_ha > 0, FALSE) as is_quilombo_identity,
        f.settlement_name, f.traditional_name, f.ti_name, f.uc_name, f.quilombo_name,
        COALESCE(so.overlap_pct, 0) as car_on_car_overlap_pct,
        COALESCE(so.total_overlapping_cars, 0) as total_overlapping_cars
    FROM properties p
    CROSS JOIN legal_params lp
    LEFT JOIN forensic_areas f ON p.property_id = f.property_id
    LEFT JOIN {{ ref('int_satellite_metrics_persistent') }} sat ON p.property_id = sat.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
    LEFT JOIN mapbiomas_check mb ON p.property_id = mb.property_id 
    LEFT JOIN slave_labor_combined sl ON p.property_id = sl.property_id
    LEFT JOIN {{ ref('int_car_compliance_metrics') }} c ON UPPER(TRIM(p.property_id)) = UPPER(TRIM(c.property_id))
    LEFT JOIN {{ ref('int_car_self_overlap') }} so ON p.property_id = so.property_id
),

analysis AS (
    SELECT
        *,
        LEAST(protected_area_overlap_ha_raw, area_ha) as protected_area_fixed_ha,
        LEAST(mapbiomas_deforested_ha_raw, area_ha) as defo_fixed_ha,
        LEAST(embargo_area_ha_raw, area_ha) as embargo_fixed_ha,
        (
            (slave_labor_match_confidence = 'HIGH') OR 
            (registration_status IN ('CANCELADO', 'SUSPENSO')) OR 
            (is_missing_geometry = TRUE) OR
            (forensic_ti_ha > 0.01 OR forensic_uc_ha > 0.01) OR 
            (forensic_quilombo_ha > 0.01 AND is_quilombo_identity IS FALSE) OR
            (forensic_settlement_ha > 0.01 AND is_settlement_identity IS FALSE) OR
            (forensic_traditional_ha > 0.01 AND is_traditional_identity IS FALSE) OR
            (mapbiomas_deforested_ha_raw > LEAST(noise_threshold, 0.1) AND (mapbiomas_date >= forest_code_date OR mapbiomas_date IS NULL)) OR 
            (embargo_area_ha_raw > 0.1 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) OR 
            -- SINCRONIA CMN 5.081 (AMAZÔNIA)
            (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha_raw >= 0.001 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) OR
            (forensic_app_hidrica_ha > 0.01 OR app_deforested_ha_forensic > 0.01) OR 
            (eudr_deforested_ha > 0.01) OR
            (max_slope_degrees > 45) OR
            (rl_deficit_ha > 0.01 AND is_small_holder IS FALSE) OR
            (EXISTS(SELECT 1 FROM UNNEST(embargo_sources) s WHERE s IN ('IBAMA', 'SEMA_MT', 'SIGA_MT', 'ICMBIO')))
        ) as is_technically_blocked
    FROM full_context
)
SELECT * FROM analysis
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['uf_origem', 'final_eligibility_status', 'biome_name'],
    tags=['compliance', 'legal', 'cmn_5081', 'eudr']
) }}

WITH legal_params AS (
    SELECT
        MAX(CASE WHEN parameter_name = 'fine_deforestation_per_ha' THEN CAST(value AS FLOAT64) END) as fine_defo,
        MAX(CASE WHEN parameter_name = 'fine_protected_area_per_ha' THEN CAST(value AS FLOAT64) END) as fine_protected,
        MAX(CASE WHEN parameter_name = 'fine_slave_labor_fixed' THEN CAST(value AS FLOAT64) END) as fine_slave,
        MAX(CASE WHEN parameter_name = 'fine_app_violation_fixed' THEN CAST(value AS FLOAT64) END) as fine_app,
        MAX(CASE WHEN parameter_name = 'fine_rl_deficit_per_ha' THEN CAST(value AS FLOAT64) END) as fine_rl,
        MAX(CASE WHEN parameter_name = 'gis_noise_ha_threshold' THEN CAST(value AS FLOAT64) END) as noise_threshold,
        MAX(CASE WHEN parameter_name = 'eudr_cutoff_date' THEN CAST(value AS DATE) END) as eudr_date,
        MAX(CASE WHEN parameter_name = 'forest_code_cutoff_date' THEN CAST(value AS DATE) END) as forest_code_date
    FROM {{ ref('legal_parameters') }}
),

properties AS (
    SELECT
        UPPER(TRIM(p_meta.property_id)) as property_id,
        COALESCE(g.area_ha, p_meta.area_ha, 0) as area_ha,
        p_meta.city,
        UPPER(TRIM(p_meta.uf_origem)) as uf_origem,
        COALESCE(TRIM(o.registration_status), 'ATIVO') as registration_status,
        g.geometry_raw as geometry,
        g.centroid,
        g.car_bbox,
        CASE WHEN g.geometry_raw IS NULL THEN TRUE ELSE FALSE END as is_missing_geometry
    FROM {{ ref('stg_car_properties') }} p_meta
    LEFT JOIN {{ ref('int_car_geometries') }} g ON UPPER(TRIM(p_meta.property_id)) = UPPER(TRIM(g.property_id))
    LEFT JOIN {{ ref('stg_car_owners') }} o ON UPPER(TRIM(p_meta.property_id)) = UPPER(TRIM(o.property_id))
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
        -- Mapeamento solicitado: Assentamentos e Territórios Tradicionais
        SUM(CASE WHEN target_type = 'RECORTE_INVASAO_ASSENTAMENTO' THEN target_area_ha ELSE 0 END) as forensic_settlement_ha,
        SUM(CASE WHEN target_type = 'RECORTE_TRADITIONAL_TERRITORY' THEN target_area_ha ELSE 0 END) as forensic_traditional_ha,
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
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT land_use_class IGNORE NULLS), ' | ') as mapbiomas_classes,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT mapbiomas_url IGNORE NULLS), ' | ') as mapbiomas_report_links
    FROM {{ ref('int_mapbiomas_deforestation') }}
    GROUP BY 1
),

full_context AS (
    SELECT
        p.*,
        lp.*,
        sat.max_slope_degrees,
        sat.relief_classification,
        sat.last_update,
        COALESCE(f.forensic_embargo_ha, 0) as embargo_area_ha,
        e.earliest_embargo_date as embargo_date,
        COALESCE(e.has_any_active_embargo, FALSE) as has_any_active_embargo,
        COALESCE(e.has_any_cancelled_embargo, FALSE) as has_any_cancelled_embargo,
        e.embargo_offenders, e.embargo_processes, e.embargo_tax_ids, e.embargo_reported_areas, e.embargo_sources,
        
        COALESCE(f.forensic_defo_ha, 0) as mapbiomas_deforested_ha,
        COALESCE(f.app_deforested_ha_forensic, 0) as app_deforested_ha_forensic,
        f.forensic_app_hidrica_ha,
        f.forensic_app_declividade_ha,
        
        mb.latest_deforestation_date as mapbiomas_date,
        mb.earliest_evidence_date, mb.latest_evidence_date,
        mb.mapbiomas_classes, mb.mapbiomas_report_links,
        
        CASE WHEN mb.latest_deforestation_date >= lp.eudr_date THEN COALESCE(f.forensic_defo_ha, 0) ELSE 0 END as eudr_deforested_ha,
        
        sl.match_confidence as slave_labor_match_confidence,
        sl.slave_labor_inclusion_date,
        COALESCE(c.biome_name, 'N/A') as biome_name, 
        c.rl_status, c.rl_deficit_ha, c.rl_balance_ha,
        
        (COALESCE(f.forensic_ti_ha, 0) + COALESCE(f.forensic_uc_ha, 0) + COALESCE(f.forensic_quilombo_ha, 0) + COALESCE(f.forensic_settlement_ha, 0) + COALESCE(f.forensic_traditional_ha, 0)) as protected_area_overlap_ha,
        CASE WHEN (COALESCE(f.forensic_ti_ha,0) + COALESCE(f.forensic_uc_ha,0) + COALESCE(f.forensic_quilombo_ha,0) + COALESCE(f.forensic_settlement_ha, 0) + COALESCE(f.forensic_traditional_ha, 0)) > 0 THEN TRUE ELSE FALSE END as is_protected_area_overlap,
        f.forensic_ti_ha, f.forensic_quilombo_ha, f.forensic_uc_ha, f.forensic_settlement_ha, f.forensic_traditional_ha, f.data_source_quality,
        
        COALESCE(so.overlap_pct, 0) as car_on_car_overlap_pct,
        COALESCE(so.total_overlapping_cars, 0) as total_overlapping_cars
    FROM properties p
    CROSS JOIN legal_params lp
    LEFT JOIN forensic_areas f ON p.property_id = f.property_id
    LEFT JOIN {{ ref('int_satellite_metrics_persistent') }} sat ON p.property_id = sat.property_id
    LEFT JOIN embargo_check e ON p.property_id = e.property_id
    LEFT JOIN mapbiomas_check mb ON p.property_id = mb.property_id 
    LEFT JOIN slave_labor_combined sl ON p.property_id = sl.property_id
    LEFT JOIN {{ ref('int_car_compliance_metrics') }} c ON p.property_id = c.property_id
    LEFT JOIN {{ ref('int_car_self_overlap') }} so ON p.property_id = so.property_id
),

final_analysis AS (
    SELECT
        *,
        LEAST(protected_area_overlap_ha, area_ha) as protected_area_fixed_ha,
        LEAST(mapbiomas_deforested_ha, area_ha) as defo_fixed_ha,
        LEAST(embargo_area_ha, area_ha) as embargo_fixed_ha,

        -- IS_TECHNICALLY_BLOCKED: Atualizado para incluir EUDR, Geometria e Assentamentos (Garante Adjacência correta)
        (
            (slave_labor_match_confidence = 'HIGH') OR 
            (registration_status IN ('CANCELADO', 'SUSPENSO')) OR 
            (is_missing_geometry = TRUE) OR
            (forensic_ti_ha > 0.001 OR forensic_quilombo_ha > 0.001 OR forensic_uc_ha > 0.001 OR forensic_settlement_ha > 0.001 OR forensic_traditional_ha > 0.001) OR 
            (mapbiomas_deforested_ha > noise_threshold AND mapbiomas_date >= forest_code_date) OR 
            (embargo_area_ha > 0.1 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) OR 
            (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha >= 0.001 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) OR
            (app_deforested_ha_forensic > 0.001) OR 
            (eudr_deforested_ha > 0) OR
            (max_slope_degrees > 45) OR
            (EXISTS(SELECT 1 FROM UNNEST(embargo_sources) s WHERE s IN ('IBAMA', 'SEMA_MT', 'SIGA_MT', 'ICMBIO')))
        ) as is_technically_blocked
    FROM full_context
),

contamination_risk AS (
    SELECT
        f1.property_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT
            CASE
                WHEN f2.slave_labor_match_confidence IS NOT NULL THEN 'SOCIAL'
                WHEN f2.embargo_area_ha > 0.1 THEN 'EMBARGO'
                WHEN f2.mapbiomas_deforested_ha > 0.1 THEN 'DESMATAMENTO'
                ELSE 'OUTROS'
            END
        ), ' | ') as neighbor_risks
    FROM final_analysis f1
    INNER JOIN final_analysis f2
        ON f1.car_bbox.xmin <= f2.car_bbox.xmax
        AND f1.car_bbox.xmax >= f2.car_bbox.xmin
        AND f1.car_bbox.ymin <= f2.car_bbox.ymax
        AND f1.car_bbox.ymax >= f2.car_bbox.ymin
    WHERE f1.property_id != f2.property_id
    AND f2.is_technically_blocked = TRUE
    AND ST_INTERSECTS(f1.geometry, f2.geometry)
    AND ST_AREA(ST_INTERSECTION(f1.geometry, f2.geometry)) > 1
    GROUP BY 1
),

final_status_calc AS (
    SELECT
        v.*,
        c.neighbor_risks as adjacency_details,
        CASE
            -- GRUPO 1: BLOQUEIOS CRÍTICOS (NOT ELIGIBLE)
            WHEN v.slave_labor_match_confidence = 'HIGH' THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
            WHEN v.registration_status IN ('CANCELADO', 'SUSPENSO') THEN 'NOT ELIGIBLE - CAR STATUS'
            WHEN v.is_missing_geometry = TRUE THEN 'NOT ELIGIBLE - INVALID GEOMETRY'
            WHEN v.forensic_ti_ha > 0 THEN 'NOT ELIGIBLE - INDIGENOUS LAND'
            WHEN v.forensic_quilombo_ha > 0 THEN 'NOT ELIGIBLE - QUILOMBOLA'
            WHEN v.forensic_uc_ha > 0 THEN 'NOT ELIGIBLE - CONSERVATION UNIT'
            WHEN v.forensic_settlement_ha > 0 THEN 'NOT ELIGIBLE - SETTLEMENT'
            WHEN v.forensic_traditional_ha > 0 THEN 'NOT ELIGIBLE - TRADITIONAL TERRITORY'
            WHEN v.forensic_app_hidrica_ha > 0 THEN 'NOT ELIGIBLE - APP DEFORESTATION (WATER)'
            WHEN v.forensic_app_declividade_ha > 0 THEN 'NOT ELIGIBLE - APP DEFORESTATION (SLOPE)'
            WHEN v.app_deforested_ha_forensic > 0 THEN 'NOT ELIGIBLE - APP DEFORESTATION'
            WHEN v.eudr_deforested_ha > 0 THEN 'NOT ELIGIBLE - EUDR VIOLATION (POST-2020)'
            WHEN v.mapbiomas_deforested_ha > v.noise_threshold AND v.mapbiomas_date >= v.forest_code_date THEN 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)'
            WHEN v.biome_name LIKE 'AMAZ%NIA' AND v.embargo_area_ha >= 0.001 AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL) THEN 'NOT ELIGIBLE - IBAMA AMAZON (CMN 5.081)'
            WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) s WHERE s = 'IBAMA') THEN 'NOT ELIGIBLE - IBAMA'
            WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) s WHERE s = 'SEMA_MT') THEN 'NOT ELIGIBLE - SEMA_MT'
            WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) s WHERE s = 'SIGA_MT') THEN 'NOT ELIGIBLE - SIGA_MT'
            WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) s WHERE s = 'ICMBIO') THEN 'NOT ELIGIBLE - ICMBIO'
            WHEN v.embargo_area_ha > 0.1 AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL) THEN 'NOT ELIGIBLE - EMBARGO'
            WHEN v.max_slope_degrees > 45 THEN 'NOT ELIGIBLE - SATELLITE (SLOPE)'

            -- FILTRO DE ESCOPO (Restaurando os 39% de propriedades não-ativas)
            WHEN v.registration_status NOT IN ('ATIVO', 'PENDENTE') THEN 'MANUAL_REVIEW_REQUIRED - OUT OF SCOPE'

            -- GRUPO 2: ALERTAS (WARNINGS & CONDITIONAL)
            WHEN c.neighbor_risks IS NOT NULL AND c.neighbor_risks != '' THEN 'WARNING - RISK BY ADJACENCY'
            WHEN v.slave_labor_match_confidence IS NOT NULL AND v.slave_labor_match_confidence != 'HIGH' THEN 'AWAITING_MANUAL_VALIDATION - POTENTIAL SOCIAL RISK'
            WHEN v.has_any_cancelled_embargo = TRUE THEN 'WARNING - EMBARGO CANCELLED/JUDICIAL'
            WHEN v.embargo_area_ha > 0.1 AND v.embargo_date >= v.forest_code_date AND v.has_any_active_embargo = FALSE THEN 'WARNING - OLD EMBARGO'
            WHEN v.mapbiomas_deforested_ha > v.noise_threshold AND v.mapbiomas_date < v.forest_code_date THEN 'WARNING - OLD EMBARGO (PRE-2008)'
            WHEN v.car_on_car_overlap_pct > 10 THEN 'WARNING - HIGH CAR OVERLAP'
            WHEN v.max_slope_degrees BETWEEN 40 AND 45 THEN 'WARNING - BORDERLINE SLOPE'
            WHEN v.rl_deficit_ha > 0.01 THEN 'CONDITIONAL - RL DEFICIT'
            
            -- ELEGÍVEL (Default para ATIVO sem riscos)
            ELSE 'ELIGIBLE'
        END as final_eligibility_status
    FROM final_analysis v
    LEFT JOIN contamination_risk c ON v.property_id = c.property_id
)

SELECT
    property_id,
    CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(property_id)), 1, 12)) as property_alias,
    area_ha,
    area_ha as property_area_ha,
    city,
    uf_origem,
    registration_status as car_status,
    biome_name,
    geometry,
    car_bbox,
    final_eligibility_status,
    is_technically_blocked,

    CASE 
        WHEN protected_area_overlap_ha > 0 AND protected_area_overlap_ha <= noise_threshold THEN 'LOW_CONFIDENCE - VECTOR ERROR (PROTECTED AREA)'
        WHEN embargo_area_ha > 0 AND embargo_area_ha <= 0.1 THEN 'LOW_CONFIDENCE - MICRO EMBARGO'
        WHEN mapbiomas_deforested_ha > 0 AND mapbiomas_deforested_ha <= noise_threshold THEN 'LOW_CONFIDENCE - MICRO DEFORESTATION'
        WHEN max_slope_degrees > 45 AND max_slope_degrees <= 46.5 THEN 'LOW_CONFIDENCE - SENSOR NOISE (SLOPE)'
        WHEN car_on_car_overlap_pct > 0 AND car_on_car_overlap_pct < 5.0 THEN 'LOW_CONFIDENCE - BOUNDARY DISPUTE'
        WHEN data_source_quality LIKE 'PREMIUM%' THEN 'HIGH_CONFIDENCE - STATE VERIFIED'
        ELSE 'HIGH_CONFIDENCE - VALID SPATIAL INTERSECTION'
    END as geospatial_confidence_level,

    embargo_fixed_ha as embargo_area_ha,
    has_any_active_embargo as is_embargo_active,
    embargo_offenders,
    embargo_processes,
    embargo_tax_ids,
    embargo_reported_areas,
    ARRAY_TO_STRING(embargo_sources, ' | ') as embargo_sources_string,

    defo_fixed_ha as mapbiomas_deforested_ha,
    eudr_deforested_ha,
    mapbiomas_classes as deforestation_types,
    mapbiomas_report_links as official_reports_urls,
    earliest_evidence_date as evidence_date_before,
    latest_evidence_date as evidence_date_after,
    CASE WHEN eudr_deforested_ha > 0 THEN TRUE ELSE FALSE END as is_eudr_restricted,

    protected_area_fixed_ha as protected_area_overlap_ha,
    protected_area_fixed_ha as protected_overlap_ha,
    is_protected_area_overlap,
    car_on_car_overlap_pct,
    CASE WHEN slave_labor_match_confidence IS NOT NULL THEN area_ha ELSE 0 END as slave_labor_overlap_ha,

    COALESCE(defo_fixed_ha * fine_defo, 0) as liability_deforestation_brl,
    COALESCE(rl_deficit_ha * fine_rl, 0) as liability_rl_brl,
    COALESCE(protected_area_fixed_ha * fine_protected, 0) as liability_protected_areas_brl,
    CASE WHEN slave_labor_match_confidence IS NOT NULL THEN fine_slave ELSE 0 END as liability_social_brl,
    CASE WHEN app_deforested_ha_forensic > 0 THEN fine_app ELSE 0 END as liability_app_brl,

    (COALESCE(defo_fixed_ha * fine_defo, 0) + 
     COALESCE(rl_deficit_ha * fine_rl, 0) + 
     COALESCE(protected_area_fixed_ha * fine_protected, 0) + 
     CASE WHEN app_deforested_ha_forensic > 0 THEN fine_app ELSE 0 END +
     CASE WHEN slave_labor_match_confidence IS NOT NULL THEN fine_slave ELSE 0 END) as estimated_financial_liability_brl,

    -- Evidências Técnicas
    ARRAY_TO_STRING(ARRAY(
        SELECT x FROM UNNEST([
            CASE WHEN slave_labor_match_confidence = 'HIGH' THEN 'SOCIAL_CRITICAL' END,
            CASE WHEN registration_status IN ('CANCELADO', 'SUSPENSO') THEN 'STATUS CAR IRREGULAR' END,
            CASE WHEN forensic_ti_ha > 0 THEN 'ÁREA INDÍGENA' END,
            CASE WHEN forensic_quilombo_ha > 0 THEN 'QUILOMBOLA' END,
            CASE WHEN forensic_uc_ha > 0 THEN 'UNIDADE DE CONSERVAÇÃO' END,
            CASE WHEN forensic_settlement_ha > 0 THEN 'ASSENTAMENTO' END,
            CASE WHEN forensic_traditional_ha > 0 THEN 'TERRITÓRIO TRADICIONAL' END,
            CASE WHEN (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha >= 0.001 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) THEN 'EMBARGO AMAZÔNIA (CMN 5.081)' END,
            CASE WHEN embargo_area_ha > 0.1 AND (embargo_date >= forest_code_date OR embargo_date IS NULL) THEN 'EMBARGO ATIVO' END,
            CASE WHEN mapbiomas_deforested_ha > noise_threshold AND mapbiomas_date >= forest_code_date THEN 'MAPBIOMAS' END,
            CASE WHEN app_deforested_ha_forensic > 0 THEN 'DESMATAMENTO EM APP' END,
            CASE WHEN eudr_deforested_ha > 0 THEN 'RESTRIÇÃO EXPORTAÇÃO (EUDR)' END,
            CASE WHEN max_slope_degrees > 45 THEN 'DECLIVIDADE' END
        ]) AS x WHERE x IS NOT NULL
    ), ' | ') as internal_risks_found,

    adjacency_details,
    CONCAT(
        "⚠️ RESTRIÇÕES: ",
        IF(final_eligibility_status LIKE 'NOT ELIGIBLE%', final_eligibility_status, "SEM BLOQUEIOS ATIVOS"),
        IF(adjacency_details IS NOT NULL, CONCAT(" | 🏠 VIZINHO: ", adjacency_details), "")
    ) as technical_evidence,
    
    ARRAY_TO_STRING(ARRAY(
        SELECT x FROM UNNEST([
            CASE WHEN has_any_cancelled_embargo = TRUE THEN 'Embargo Cancelado' END,
            CASE WHEN mapbiomas_deforested_ha > noise_threshold AND mapbiomas_date < forest_code_date THEN 'Desmatamento Pré-2008' END,
            CASE WHEN rl_deficit_ha > 0.01 THEN 'Déficit de RL' END
        ]) AS x WHERE x IS NOT NULL
    ), ' | ') as historical_warnings,

    mapbiomas_date as mapbiomas_detection_date,
    COALESCE(embargo_date, CAST('1900-01-01' AS DATE)) as embargo_date,
    slave_labor_inclusion_date,
    CURRENT_TIMESTAMP() as analyzed_at,
    last_update as processed_at,
    ST_Y(centroid) as latitude,
    ST_X(centroid) as longitude,

    max_slope_degrees,
    relief_classification,
    rl_status,
    rl_deficit_ha,
    rl_balance_ha

FROM final_status_calc
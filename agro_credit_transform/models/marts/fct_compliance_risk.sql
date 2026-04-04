-- models/marts/fct_compliance_risk.sql
{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['uf_origem', 'final_eligibility_status', 'biome_name'],
    tags=['compliance', 'legal', 'cmn_5081', 'eudr']
) }}

WITH base AS (
    SELECT * FROM {{ ref('int_compliance__property_analysis') }}
),

adjacencies AS (
    SELECT * FROM {{ ref('int_compliance__adjacency_scoring') }}
),

final_status_calc AS (
    SELECT
        v.*,
        CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
        adj.adjacency_risk_types as adjacency_details,
        adj.max_adjacency_score,

        CASE
            -- 1. BLOQUEIOS LEGAIS E SOCIAIS (HARD BLOCKS)
            WHEN v.slave_labor_match_confidence = 'HIGH' THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
            
            -- Se qualquer fonte oficial cancelar o CAR, o status é NOT ELIGIBLE (Regra 1 do Teste)
            WHEN v.registration_status IN ('CANCELADO', 'SUSPENSO') 
                 OR v.registration_status_geometry IN ('CANCELADO', 'SUSPENSO') 
                 THEN 'NOT ELIGIBLE - CAR STATUS'
            
            WHEN v.is_missing_geometry = TRUE THEN 'NOT ELIGIBLE - INVALID GEOMETRY'
            
            WHEN v.forensic_ti_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_ti_identity IS FALSE THEN 'NOT ELIGIBLE - INDIGENOUS LAND'
            WHEN v.forensic_uc_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_uc_identity IS FALSE THEN 'NOT ELIGIBLE - CONSERVATION UNIT'
            WHEN v.forensic_quilombo_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_quilombo_identity IS FALSE THEN 'NOT ELIGIBLE - QUILOMBOLA (INVASION)'
            WHEN v.forensic_settlement_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_settlement_identity IS FALSE THEN 'NOT ELIGIBLE - SETTLEMENT (INVASION)'
            WHEN v.forensic_traditional_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_traditional_identity IS FALSE THEN 'NOT ELIGIBLE - TRADITIONAL TERRITORY (INVASION)'
            
            WHEN v.forensic_app_hidrica_ha > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - APP DEFORESTATION (WATER)'
            WHEN v.app_deforested_ha_forensic > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - APP DEFORESTATION'
            WHEN v.eudr_deforested_ha > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - EUDR VIOLATION (POST-2020)'
            
            WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) 
                 AND v.mapbiomas_date >= v.forest_code_date 
                 THEN 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)'

            -- 2. Desmatamento sem Prova de Data (Bloqueio por precaução/falta de evidência)
            WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) 
                 AND v.mapbiomas_date IS NULL 
                 THEN 'NOT ELIGIBLE - DEFORESTATION (MISSING DATE EVIDENCE)'

            WHEN v.biome_name LIKE 'AMAZ%NIA' AND v.embargo_area_ha_raw >= 0.001 AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL)
                 THEN 'NOT ELIGIBLE - AMZ EMBARGO (CMN 5.081)'
            
            WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) s WHERE s IN ('IBAMA', 'SEMA_MT', 'SIGA_MT', 'ICMBIO')) 
                 THEN 'NOT ELIGIBLE - EMBARGO (OFFICIAL SOURCE)'
            
            WHEN v.embargo_area_ha_raw > 0.1 AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL) 
                 THEN 'NOT ELIGIBLE - EMBARGO'
            
            WHEN v.max_slope_degrees > 45 THEN 'NOT ELIGIBLE - SATELLITE (SLOPE)'
            
            WHEN v.rl_deficit_ha > 0.01 AND v.is_small_holder IS FALSE AND (v.solicitacao_adesao_pra IS NULL OR v.solicitacao_adesao_pra != 'Sim')
                 THEN 'NOT ELIGIBLE - RL DEFICIT'
            
            WHEN v.area_ha > 2000000 THEN 'MANUAL_REVIEW_REQUIRED - UNREALISTIC AREA'
            
            WHEN (ST_AREA(v.geometry) / 10000) > (v.area_ha * 1.50) 
                 THEN 'MANUAL_REVIEW_REQUIRED - POTENTIAL AREA FRAUD'

            -- 2. REVISÃO MANUAL (DADOS INCONSISTENTES OU PENDENTES)
            -- Se chegou aqui, não é Cancelado nem Suspenso.
            WHEN v.registration_status = 'INCONSISTENTE' OR v.registration_status_geometry = 'INCONSISTENTE' 
                 THEN 'MANUAL_REVIEW_REQUIRED - INCONSISTENT CAR STATUS'
            
            WHEN v.registration_status != v.registration_status_geometry 
                 THEN 'MANUAL_REVIEW_REQUIRED - DATA SOURCE CONFLICT'

            WHEN (v.is_settlement_identity OR v.is_traditional_identity OR v.is_quilombo_identity OR v.is_ti_identity OR v.is_uc_identity) AND v.area_ha > 1000 
                 THEN 'MANUAL_REVIEW_REQUIRED - UNUSUAL AREA FOR IDENTITY'
            
            -- 3. ELEGIBILIDADE POR IDENTIDADE (EXCEÇÕES DO TESTE)
            WHEN v.is_ti_identity IS TRUE THEN 'ELIGIBLE - INDIGENOUS PRODUCER'
            WHEN v.is_uc_identity IS TRUE THEN 'ELIGIBLE - CONSERVATION UNIT PRODUCER'
            WHEN v.is_quilombo_identity IS TRUE THEN 'ELIGIBLE - QUILOMBOLA PRODUCER'
            WHEN v.is_settlement_identity IS TRUE THEN 'ELIGIBLE - SETTLEMENT PRODUCER'
            WHEN v.is_traditional_identity IS TRUE THEN 'ELIGIBLE - TRADITIONAL PRODUCER'
            
            -- 4. OUTROS STATUS DE REVISÃO
            WHEN v.registration_status = 'PENDENTE' THEN 'MANUAL_REVIEW_REQUIRED - PENDING STATE ANALYSIS'
            WHEN v.registration_status NOT IN ('ATIVO') THEN 'MANUAL_REVIEW_REQUIRED - OUT OF SCOPE'
            
            WHEN adj.max_adjacency_score >= 70 THEN 'WARNING - CRITICAL ADJACENCY RISK'
            WHEN adj.max_adjacency_score > 0 THEN 'WARNING - RISK BY ADJACENCY'
            
            WHEN v.slave_labor_match_confidence IS NOT NULL AND v.slave_labor_match_confidence != 'HIGH' THEN 'AWAITING_MANUAL_VALIDATION - POTENTIAL SOCIAL RISK'
            WHEN v.has_any_cancelled_embargo = TRUE THEN 'WARNING - EMBARGO CANCELLED/JUDICIAL'
            WHEN v.car_on_car_overlap_pct > 10 THEN 'WARNING - HIGH CAR OVERLAP'
            WHEN v.rl_deficit_ha > 0.01 AND (v.is_small_holder IS TRUE OR v.solicitacao_adesao_pra = 'Sim') THEN 'CONDITIONAL - RL REGULARIZATION'
            
            ELSE 'ELIGIBLE'
        END as final_eligibility_status,

        (
          COALESCE(v.defo_fixed_ha * v.fine_defo, 0) + 
          (CASE WHEN v.is_small_holder OR v.solicitacao_adesao_pra = 'Sim' THEN 0 ELSE COALESCE(v.rl_deficit_ha * v.fine_rl, 0) END) + 
          (CASE WHEN v.is_settlement_identity OR v.is_traditional_identity OR v.is_quilombo_identity OR v.is_ti_identity OR v.is_uc_identity THEN 0 ELSE COALESCE(v.protected_area_fixed_ha * v.fine_protected, 0) END) + 
          COALESCE(v.embargo_fixed_ha * v.fine_embargo, 0) + 
          CASE WHEN (v.app_deforested_ha_forensic > 0.01 OR v.forensic_app_hidrica_ha > 0.01) THEN v.fine_app ELSE 0 END +
          CASE WHEN v.slave_labor_match_confidence = 'HIGH' THEN v.fine_slave ELSE 0 END
        ) as estimated_financial_liability_brl

    FROM base v
    LEFT JOIN adjacencies adj ON v.property_id = adj.property_id
)

SELECT
    property_id, property_alias, area_ha, area_ha as property_area_ha, area_liquida_ha,
    fiscal_modules, city, uf_origem, registration_status as car_status, solicitacao_adesao_pra,
    biome_name, geometry, car_bbox, final_eligibility_status, is_technically_blocked, is_missing_geometry,

    CASE 
        WHEN defo_fixed_ha > 0.1 AND mapbiomas_official_alert_ha > 0.1 THEN 'ULTRA_HIGH - DOUBLE_VERIFIED (GIS + MAPBIOMAS)'
        WHEN protected_area_fixed_ha > 0 AND protected_area_fixed_ha <= 0.01 THEN 'LOW_CONFIDENCE - VECTOR ERROR (PROTECTED AREA)'
        WHEN embargo_fixed_ha > 0 AND embargo_fixed_ha <= 0.1 THEN 'LOW_CONFIDENCE - MICRO EMBARGO'
        WHEN defo_fixed_ha > 0 AND defo_fixed_ha <= noise_threshold THEN 'LOW_CONFIDENCE - MICRO DEFORESTATION'
        WHEN max_slope_degrees > 45 AND max_slope_degrees <= 46.5 THEN 'LOW_CONFIDENCE - SENSOR NOISE (SLOPE)'
        WHEN car_on_car_overlap_pct > 0 AND car_on_car_overlap_pct < 5.0 THEN 'LOW_CONFIDENCE - BOUNDARY DISPUTE'
        WHEN data_source_quality LIKE 'PREMIUM%' THEN 'HIGH_CONFIDENCE - STATE VERIFIED'
        ELSE 'HIGH_CONFIDENCE - VALID SPATIAL INTERSECTION'
    END as geospatial_confidence_level,

    embargo_fixed_ha as embargo_area_ha, has_any_active_embargo as is_embargo_active,
    embargo_offenders, embargo_processes, embargo_tax_ids, embargo_reported_areas,
    ARRAY_TO_STRING(embargo_sources, ' | ') as embargo_sources_string,

    defo_fixed_ha as mapbiomas_deforested_ha, eudr_deforested_ha, mapbiomas_classes as deforestation_types,
    mapbiomas_report_links as official_reports_urls, earliest_evidence_date as evidence_date_before,
    latest_evidence_date as evidence_date_after, (eudr_deforested_ha > 0.01) as is_eudr_restricted,

    CASE WHEN is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity THEN 0 ELSE protected_area_fixed_ha END as protected_area_overlap_ha,
    (protected_area_fixed_ha > 0.01 AND NOT (is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity)) as is_protected_area_overlap,
    area_rural_consolidada_ha, area_pousio_ha, area_uso_restrito_ha,
    car_on_car_overlap_pct, CASE WHEN slave_labor_match_confidence = 'HIGH' THEN area_ha ELSE 0 END as slave_labor_overlap_ha,

    COALESCE(defo_fixed_ha * fine_defo, 0) as liability_deforestation_brl,
    CASE WHEN is_small_holder OR solicitacao_adesao_pra = 'Sim' THEN 0 ELSE COALESCE(rl_deficit_ha * fine_rl, 0) END as liability_rl_brl,
    COALESCE(embargo_fixed_ha * fine_embargo, 0) as liability_embargo_brl,
    CASE WHEN is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity THEN 0 ELSE COALESCE(protected_area_fixed_ha * fine_protected, 0) END as liability_protected_areas_brl,
    CASE WHEN slave_labor_match_confidence = 'HIGH' THEN fine_slave ELSE 0 END as liability_social_brl,
    CASE WHEN (app_deforested_ha_forensic > 0.01 OR forensic_app_hidrica_ha > 0.01) THEN fine_app ELSE 0 END as liability_app_brl,

    estimated_financial_liability_brl,

    ARRAY_TO_STRING(ARRAY(
        SELECT x FROM UNNEST([
            CASE WHEN slave_labor_match_confidence = 'HIGH' THEN 'SOCIAL_CRITICAL' END,
            CASE WHEN (registration_status IN ('CANCELADO', 'SUSPENSO') OR registration_status_geometry IN ('CANCELADO', 'SUSPENSO')) THEN 'STATUS CAR IRREGULAR' END,
            CASE WHEN (registration_status = 'INCONSISTENTE' OR registration_status_geometry = 'INCONSISTENTE' OR registration_status != registration_status_geometry) THEN 'INCONSISTÊNCIA DE DADOS (CAR)' END,
            CASE WHEN forensic_ti_ha > 0.01 AND is_ti_identity IS FALSE THEN CONCAT('TI ', COALESCE(ti_name, 'N/A')) END,
            CASE WHEN forensic_quilombo_ha > 0.01 AND is_quilombo_identity IS FALSE THEN CONCAT('QUILOMBO ', COALESCE(quilombo_name, 'N/A')) END,
            CASE WHEN forensic_uc_ha > 0.01 AND is_uc_identity IS FALSE THEN CONCAT('UC ', COALESCE(uc_name, 'N/A')) END,
            CASE WHEN forensic_settlement_ha > 0.01 AND is_settlement_identity IS FALSE THEN CONCAT('ASSENTAMENTO ', COALESCE(settlement_name, 'N/A')) END,
            CASE WHEN forensic_traditional_ha > 0.01 AND is_traditional_identity IS FALSE THEN CONCAT('TERRITÓRIO ', COALESCE(traditional_name, 'N/A')) END,
            CASE WHEN (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha_raw >= 0.001 AND (embargo_date >= forest_code_date OR embargo_date IS NULL)) THEN 'EMBARGO AMAZÔNIA (CMN 5.081)' END,
            CASE WHEN embargo_area_ha_raw > 0.1 AND (embargo_date >= forest_code_date OR embargo_date IS NULL) THEN 'EMBARGO ATIVO' END,
            CASE WHEN defo_fixed_ha > noise_threshold AND (mapbiomas_date >= forest_code_date OR mapbiomas_date IS NULL) THEN 'MAPBIOMAS' END,
            CASE WHEN (app_deforested_ha_forensic > 0.01 OR forensic_app_hidrica_ha > 0.01) THEN 'DESMATAMENTO EM APP' END,
            CASE WHEN eudr_deforested_ha > 0.01 THEN 'RESTRIÇÃO EXPORTAÇÃO (EUDR)' END,
            CASE WHEN max_slope_degrees > 45 THEN 'DECLIVIDADE' END
        ]) AS x WHERE x IS NOT NULL
    ), ' | ') as internal_risks_found,

    adjacency_details, max_adjacency_score,

    CONCAT(
        IF(final_eligibility_status LIKE '%PRODUCER%', "✅ OPERAÇÃO LEGALIZADA: ", "⚠️ RESTRIÇÕES: "),
        IF(final_eligibility_status LIKE 'NOT ELIGIBLE%' OR final_eligibility_status LIKE '%PRODUCER%' OR final_eligibility_status LIKE 'MANUAL_REVIEW%', final_eligibility_status, "SEM BLOQUEIOS ATIVOS"),
        IF(solicitacao_adesao_pra = 'Sim', " | 📝 ADERIU AO PRA", ""),
        IF(registration_status != registration_status_geometry, " | ❗ DIVERGÊNCIA ENTRE BASES CAR", ""),
        IF(adjacency_details IS NOT NULL, CONCAT(" | 🏠 VIZINHO (Score ", CAST(max_adjacency_score AS STRING), "): ", adjacency_details), ""),
        IF(mapbiomas_alert_ids != '', CONCAT(" | 📄 Alertas MapBiomas: ", mapbiomas_alert_ids), ""),
        IF(is_ti_identity, CONCAT(" | ✅ Cadastro de Terra Indígena: ", ti_name), ""),
        IF(is_uc_identity, CONCAT(" | ✅ Cadastro de Unidade de Conservação: ", uc_name), ""),
        IF(is_settlement_identity, CONCAT(" | ✅ Assentado no PA: ", settlement_name), ""),
        IF(is_quilombo_identity, CONCAT(" | ✅ Quilombola no Território: ", quilombo_name), ""),
        IF(forensic_ti_ha > 0.01 AND NOT is_ti_identity, CONCAT(" | ⚠️ Invasão de TI: ", ti_name), ""),
        IF(forensic_uc_ha > 0.01 AND NOT is_uc_identity, CONCAT(" | ⚠️ Invasão de UC: ", uc_name), ""),
        IF(forensic_settlement_ha > 0.01 AND NOT is_settlement_identity, CONCAT(" | ⚠️ Invasão de Assentamento: ", settlement_name), ""),
        IF(forensic_quilombo_ha > 0.01 AND NOT is_quilombo_identity, CONCAT(" | ⚠️ Invasão de Quilombo: ", quilombo_name), "")
    ) as technical_evidence,

    ARRAY_TO_STRING(ARRAY(
        SELECT x FROM UNNEST([
            CASE WHEN has_any_cancelled_embargo = TRUE THEN 'Embargo Cancelado' END,
            CASE WHEN defo_fixed_ha > noise_threshold AND mapbiomas_date < forest_code_date THEN 'Desmatamento Pré-2008' END,
            CASE WHEN rl_deficit_ha > 0.01 THEN 'Déficit de RL' END
        ]) AS x WHERE x IS NOT NULL
    ), ' | ') as historical_warnings,

    COALESCE(mapbiomas_date, '1900-01-01') as mapbiomas_detection_date,
    COALESCE(embargo_date, '1900-01-01') as embargo_date,
    slave_labor_inclusion_date, CURRENT_TIMESTAMP() as analyzed_at, last_update as processed_at,
    ST_Y(centroid) as latitude, ST_X(centroid) as longitude,
    mapbiomas_alert_ids, mapbiomas_official_alert_ha as official_alert_area_ha,
    mapbiomas_official_ti_ha, mapbiomas_official_quilombo_ha, mapbiomas_official_settlement_ha,
    forensic_ti_ha, forensic_quilombo_ha, forensic_uc_ha, forensic_settlement_ha, forensic_traditional_ha,
    forensic_app_hidrica_ha, forensic_app_declividade_ha,
    is_ti_identity, is_uc_identity, is_settlement_identity, is_traditional_identity, is_quilombo_identity,
    max_slope_degrees, relief_classification, rl_status, rl_deficit_ha, rl_balance_ha,
    producer_size_category, is_small_holder, fmp_ha

FROM final_status_calc
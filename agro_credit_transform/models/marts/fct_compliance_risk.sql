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
adj.adjacent_roads,

-- [NOVO] Cálculo de Confiabilidade
(100 
    - (CASE WHEN (ST_AREA(v.geometry) / 10000) > (v.area_ha * 1.50) THEN 30 ELSE 0 END)
    - (CASE WHEN v.mapbiomas_date IS NULL OR v.embargo_date IS NULL THEN 20 ELSE 0 END)
    - (CASE WHEN v.registration_status != v.registration_status_geometry THEN 10 ELSE 0 END)
) as data_reliability_index,

-- [NOVO] Gavetas de Evidência (Arrays Modulares)
ARRAY(
    SELECT x FROM UNNEST([
        CASE WHEN v.registration_status IN ('CANCELADO', 'SUSPENSO') OR v.registration_status_geometry IN ('CANCELADO', 'SUSPENSO') THEN '[SICAR] Status do CAR Irregular ou Cancelado' END,
        CASE WHEN v.registration_status = 'INCONSISTENTE' OR v.registration_status_geometry = 'INCONSISTENTE' THEN '[SICAR] Inconsistência detectada na base oficial' END,
        CASE WHEN v.registration_status != v.registration_status_geometry THEN '[SICAR] Conflito de informações entre bases alfanumérica e geográfica' END,
        CONCAT('[SISTEMA] ÁREA: Processada ', v.area_ha, ' | Original: ', COALESCE(CAST(v.area_ha_original AS STRING), 'N/A'))
    ]) AS x WHERE x IS NOT NULL
) as evidence_admin_array,

ARRAY(
    SELECT x FROM UNNEST([
        CASE WHEN v.slave_labor_match_confidence = 'HIGH' THEN '[MTE] Trabalho Escravo: Confirmação de lista suja' END,
        CASE WHEN v.forensic_ti_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_ti_identity IS FALSE THEN '[FUNAI] Sobreposição com Terra Indígena' END,
        CASE WHEN v.forensic_quilombo_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_quilombo_identity IS FALSE THEN '[INCRA] Sobreposição com Território Quilombola' END,
        CASE WHEN v.forensic_settlement_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_settlement_identity IS FALSE THEN '[INCRA] Sobreposição com Assentamento' END
    ]) AS x WHERE x IS NOT NULL
) as evidence_social_array,

ARRAY(
    SELECT x FROM UNNEST([
        CASE WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) THEN '[MAPBIOMAS] Alerta de desmatamento detectado' END,
        CASE WHEN v.embargo_area_ha_raw > 0.1 THEN '[IBAMA] Embargo ambiental ativo na área' END,
        CASE WHEN v.eudr_deforested_ha > {{ var('gis_noise_ha_threshold') }} THEN '[EUDR] Violação de desmatamento pós-2020' END,
        CASE WHEN v.forensic_app_hidrica_ha > {{ var('gis_noise_ha_threshold') }} THEN '[SISTEMA] Desmatamento em APP Hídrica' END
    ]) AS x WHERE x IS NOT NULL
) as evidence_environmental_array,

ARRAY(
    SELECT x FROM UNNEST([
        CASE WHEN v.max_slope_degrees > 45 THEN '[SISTEMA] Declividade superior a 45 graus' END,
        CASE WHEN adj.max_adjacency_score > 0 THEN '[SISTEMA] Risco por Adjacência detectado' END,
        CASE WHEN v.road_fixed_ha > 0.01 THEN '[DNIT] Sobreposição com Faixa de Domínio' END
    ]) AS x WHERE x IS NOT NULL
) as evidence_infrastructure_array,

CASE
        -- 1. BLOQUEIOS LEGAIS E SOCIAIS (HARD BLOCKS)
        WHEN v.slave_labor_match_confidence = 'HIGH' THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
        
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

        -- Ajuste Marco Temporal MapBiomas
        WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) 
             AND (v.mapbiomas_date IS NULL OR v.mapbiomas_date = '1900-01-01')
             THEN 'MANUAL_REVIEW - UNDATED DEFORESTATION'

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

        -- 2. REVISÃO MANUAL (DADOS INCONSISTENTES)
        WHEN v.registration_status = 'INCONSISTENTE' OR v.registration_status_geometry = 'INCONSISTENTE' 
             THEN 'MANUAL_REVIEW_REQUIRED - INCONSISTENT CAR STATUS'
        
        WHEN v.registration_status != v.registration_status_geometry 
             THEN 'MANUAL_REVIEW_REQUIRED - DATA SOURCE CONFLICT'

        WHEN (v.is_settlement_identity OR v.is_traditional_identity OR v.is_quilombo_identity OR v.is_ti_identity OR v.is_uc_identity) AND v.area_ha > 1000 
             THEN 'MANUAL_REVIEW_REQUIRED - UNUSUAL AREA FOR IDENTITY'

        -- 3. ELEGIBILIDADE POR IDENTIDADE
        WHEN v.is_ti_identity IS TRUE THEN 'ELIGIBLE - INDIGENOUS PRODUCER'
        WHEN v.is_uc_identity IS TRUE THEN 'ELIGIBLE - CONSERVATION UNIT PRODUCER'
        WHEN v.is_quilombo_identity IS TRUE THEN 'ELIGIBLE - QUILOMBOLA PRODUCER'
        WHEN v.is_settlement_identity IS TRUE THEN 'ELIGIBLE - SETTLEMENT PRODUCER'
        WHEN v.is_traditional_identity IS TRUE THEN 'ELIGIBLE - TRADITIONAL PRODUCER'
        
        -- 4. STATUS ADMINISTRATIVO PENDENTE (SUBIU NA HIERARQUIA PARA SATISFAZER O TESTE)
        WHEN v.registration_status = 'PENDENTE' THEN 'MANUAL_REVIEW_REQUIRED - PENDING STATE ANALYSIS'
        WHEN v.registration_status NOT IN ('ATIVO') THEN 'MANUAL_REVIEW_REQUIRED - OUT OF SCOPE'

        -- 5. ALERTAS DE ADJACÊNCIA E LAVAGEM DE GRÃOS (SÓ APLICADOS SE O CAR FOR ATIVO)
        WHEN adj.adjacency_risk_types LIKE '%LAUNDERING_RISK%' THEN 'WARNING - CRITICAL ADJACENCY RISK (LAUNDERING)'
        WHEN adj.max_adjacency_score >= 70 THEN 'WARNING - CRITICAL ADJACENCY RISK'
        WHEN adj.max_adjacency_score > 0 THEN 'WARNING - RISK BY ADJACENCY'
        
        -- 6. OUTROS AVISOS
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
),

-- [NOVO] CTE de Formatação e Consolidação
final_formatting AS (
    SELECT 
        *,
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

        ARRAY_TO_STRING(ARRAY_CONCAT(evidence_admin_array, evidence_social_array, evidence_environmental_array, evidence_infrastructure_array), ' | ') as technical_evidence_consolidated,
        ARRAY_TO_STRING(ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN slave_labor_match_confidence = 'HIGH' THEN 'SOCIAL_CRITICAL' END,
                CASE WHEN (registration_status IN ('CANCELADO', 'SUSPENSO') OR registration_status_geometry IN ('CANCELADO', 'SUSPENSO')) THEN 'STATUS CAR IRREGULAR' END,
                CASE WHEN (registration_status = 'INCONSISTENTE' OR registration_status_geometry = 'INCONSISTENTE' OR registration_status != registration_status_geometry) THEN 'INCONSISTÊNCIA DE DADOS (CAR)' END,
                CASE WHEN forensic_ti_ha > 0.01 AND is_ti_identity IS FALSE THEN 'NOT ELIGIBLE - INDIGENOUS LAND' END,
                CASE WHEN forensic_quilombo_ha > 0.01 AND is_quilombo_identity IS FALSE THEN 'NOT ELIGIBLE - QUILOMBOLA' END,
                CASE WHEN forensic_uc_ha > 0.01 AND is_uc_identity IS FALSE THEN 'NOT ELIGIBLE - CONSERVATION UNIT' END,
                CASE WHEN (mapbiomas_deforested_ha_raw > 0.1 AND (mapbiomas_date IS NULL OR mapbiomas_date = '1900-01-01')) THEN 'MANUAL_REVIEW - UNDATED DEFORESTATION' END,
                CASE WHEN eudr_deforested_ha > 0.01 THEN 'NOT ELIGIBLE - EUDR VIOLATION' END,
                CASE WHEN (ST_AREA(geometry) / 10000) > (area_ha * 1.50) THEN 'MANUAL_REVIEW_REQUIRED - POTENTIAL AREA FRAUD' END
            ]) AS x WHERE x IS NOT NULL
        ), ' + ') as final_eligibility_status_detailed
    FROM final_status_calc
)

SELECT
property_id, property_alias, area_ha, area_ha as property_area_ha, area_liquida_ha,
fiscal_modules, city, uf_origem, registration_status as car_status, solicitacao_adesao_pra,
biome_name, geometry, car_bbox, final_eligibility_status, is_technically_blocked, is_missing_geometry,

geospatial_confidence_level,

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

road_fixed_ha as road_overlap_ha,
road_names,

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
        CASE WHEN road_fixed_ha > 0.01 THEN 'SOBREPOSIÇÃO FAIXA DE DOMÍNIO' END,
        CASE WHEN adjacency_details LIKE '%LAUNDERING_RISK%' THEN 'RISCO LAVAGEM DE GRÃOS' END,
        CASE WHEN max_slope_degrees > 45 THEN 'DECLIVIDADE' END
    ]) AS x WHERE x IS NOT NULL
), ' | ') as internal_risks_found,

adjacency_details, max_adjacency_score, adjacent_roads,

-- [AJUSTADO] technical_evidence agora consome os arrays modulares via CTE de formatação
technical_evidence_consolidated as technical_evidence,

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
producer_size_category, is_small_holder, fmp_ha,

-- [NOVAS COLUNAS EXPOSTAS]
data_reliability_index,
((ST_AREA(geometry) / 10000) > (area_ha * 1.50)) as is_liability_uncertain,
ARRAY_TO_STRING(evidence_admin_array, ' | ') as evidence_admin,
ARRAY_TO_STRING(evidence_social_array, ' | ') as evidence_social,
ARRAY_TO_STRING(evidence_environmental_array, ' | ') as evidence_environmental,
ARRAY_TO_STRING(evidence_infrastructure_array, ' | ') as evidence_infrastructure,
CONCAT('Status: ', final_eligibility_status, ' | Confiança: ', geospatial_confidence_level, ' | Índice: ', data_reliability_index) as forensic_summary,
final_eligibility_status_detailed

FROM final_formatting
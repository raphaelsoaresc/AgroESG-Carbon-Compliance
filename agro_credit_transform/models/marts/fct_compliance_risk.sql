{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['uf_origem', 'final_eligibility_status', 'biome_name'],
    tags=['gold', 'marts', 'bi', 'compliance', 'legal', 'cmn_5081', 'eudr']
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
        -- Cálculo de percentuais de sobreposição para o laudo técnico
        ROUND(SAFE_DIVIDE(v.forensic_ti_ha, v.area_ha) * 100, 2) as ti_overlap_pct,
        ROUND(SAFE_DIVIDE(v.forensic_uc_ha, v.area_ha) * 100, 2) as uc_overlap_pct,
        ROUND(SAFE_DIVIDE(v.forensic_settlement_ha, v.area_ha) * 100, 2) as settlement_overlap_pct,
        ROUND(SAFE_DIVIDE(v.forensic_traditional_ha, v.area_ha) * 100, 2) as traditional_overlap_pct,

        -- Classificação consolidada de Identidade da Propriedade
        CASE 
            -- Casos Declarados (Dado Público Puro)
            WHEN v.property_type = 'TI' THEN 'Terra Indígena (Declarado)'
            WHEN v.property_type = 'AST' THEN 'Assentamento (Declarado)'
            WHEN v.property_type = 'PCT' AND v.quilombo_name IS NOT NULL THEN 'Quilombola (Declarado)'
            
            -- Casos de Resgate (Inteligência do Sistema para evitar erro de Invasão)
            WHEN v.is_ti_identity AND v.property_type = 'IRU' THEN 'Terra Indígena (Resgate por Localização)'
            WHEN v.is_quilombo_identity AND v.property_type = 'IRU' THEN 'Quilombola (Resgate por Localização)'
            WHEN v.is_settlement_identity AND v.property_type = 'IRU' THEN 'Assentamento (Resgate por Localização)'
            
            WHEN v.property_type = 'PCT' THEN 'Território Tradicional (Declarado)'
            ELSE 'Imóvel Rural Privado'
        END as property_identity_type,

        -- Identidade visual e metadados de vizinhança
        CONCAT('Fazenda ', SUBSTR(TO_HEX(MD5(v.property_id)), 1, 12)) as property_alias,
        adj.adjacency_risk_types as adjacency_details,
        adj.max_adjacency_score,
        adj.adjacent_roads,
        adj.has_physical_barrier,
        adj.adjacent_rivers,
        adj.critical_contact_point,

        -- Índice de Confiabilidade dos Dados (Penalidades por inconsistências forenses)
        (100 
            - (CASE WHEN v.is_area_inconsistent OR (ST_AREA(v.geometry) / 10000) > (v.area_ha * 1.50) THEN 30 ELSE 0 END)
            - (CASE WHEN v.mapbiomas_date IS NULL OR v.embargo_date IS NULL THEN 20 ELSE 0 END)
            - (CASE WHEN v.registration_status != v.registration_status_geometry THEN 10 ELSE 0 END)
        ) as data_reliability_index,

        -- GAVETA 1: EVIDÊNCIAS ADMINISTRATIVAS E DOCUMENTAIS
        ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN v.registration_status IN ('CANCELADO', 'SUSPENSO') OR v.registration_status_geometry IN ('CANCELADO', 'SUSPENSO') THEN '[SICAR] Status do CAR Irregular ou Cancelado' END,
                CASE WHEN v.registration_status = 'INCONSISTENTE' OR v.registration_status_geometry = 'INCONSISTENTE' THEN '[SICAR] Inconsistência detectada na base oficial' END,
                CASE WHEN v.registration_status != v.registration_status_geometry THEN '[SICAR] Conflito de informações entre bases alfanumérica e geográfica' END,
                CASE WHEN v.is_area_inconsistent THEN '[SISTEMA] Inconsistência de área detectada na origem' END,
                CASE WHEN v.city_data_source_origin = 'RECUPERADO: BASE GEOMETRIA' 
                     THEN '[SISTEMA] Cidade ausente na base de temas; recuperada via base de geometria' END,
                CASE WHEN v.city_data_source_origin = 'NÃO INFORMADO PELO GOVERNO' 
                     THEN '[ALERTA] Localidade (Cidade) não consta em nenhuma base oficial do governo' END,
                CONCAT('[SISTEMA] ÁREA: Processada ', v.area_ha, ' | Original: ', COALESCE(CAST(v.area_ha_original AS STRING), 'N/A'))
            ]) AS x WHERE x IS NOT NULL
        ) as evidence_admin_array,

        -- GAVETA 2: EVIDÊNCIAS SOCIAIS E TERRITORIAIS
        ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN v.slave_labor_match_confidence = 'HIGH' THEN '[MTE] Trabalho Escravo: Confirmação de lista suja' END,
                CASE WHEN v.forensic_ti_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_ti_identity IS FALSE THEN '[FUNAI] Sobreposição com Terra Indígena' END,
                CASE WHEN v.forensic_ti_ha > 0.01 THEN CONCAT('[FUNAI] Sobreposição TI: ', ROUND(SAFE_DIVIDE(v.forensic_ti_ha, v.area_ha) * 100, 2), '%') END,
                CASE WHEN v.forensic_quilombo_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_quilombo_identity IS FALSE THEN '[INCRA] Sobreposição com Território Quilombola' END,
                CASE WHEN v.forensic_settlement_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_settlement_identity IS FALSE THEN '[INCRA] Sobreposição com Assentamento' END
            ]) AS x WHERE x IS NOT NULL
        ) as evidence_social_array,

        -- GAVETA 3: EVIDÊNCIAS AMBIENTAIS (VERDADE DE SOLO)
        ARRAY(
            SELECT x FROM UNNEST([
                CASE 
                    WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) 
                    THEN CONCAT('[MAPBIOMAS] Desmatamento de ', COALESCE(v.mapbiomas_classes, 'vegetação'), ' detectado em ', CAST(v.mapbiomas_date AS STRING)) 
                END,
                CASE WHEN v.embargo_area_ha_raw > 0.1 THEN '[IBAMA] Embargo ambiental ativo na área' END,
                CASE WHEN v.eudr_deforested_ha > {{ var('gis_noise_ha_threshold') }} THEN '[EUDR] Violação de desmatamento pós-2020' END,
                CASE WHEN v.forensic_app_hidrica_ha > {{ var('gis_noise_ha_threshold') }} THEN '[SISTEMA] Desmatamento em APP Hídrica' END,
                CASE WHEN v.count_artificial_water_bodies > 0 THEN CONCAT('[SISTEMA] Alteração Hídrica: ', v.count_artificial_water_bodies, ' ponto(s) detectado(s)') END,
                CASE WHEN v.is_structured_environmental_risk THEN '[ALERTA] Risco Ambiental Estruturado (Crime + Logística)' END
            ]) AS x WHERE x IS NOT NULL
        ) as evidence_environmental_array,

        -- GAVETA 4: EVIDÊNCIAS DE INFRAESTRUTURA E LOGÍSTICA
        ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN v.max_slope_degrees > 45 THEN '[SISTEMA] Declividade superior a 45 graus' END,
                CASE WHEN adj.max_adjacency_score > 0 THEN '[SISTEMA] Risco por Adjacência detectado' END,
                CASE WHEN v.road_fixed_ha > 0.01 
                    THEN CONCAT('[DNIT] Sobreposição com Faixa de Domínio da rodovia: ', v.road_names) 
                END,
                CASE WHEN adj.has_physical_barrier IS TRUE THEN CONCAT('[MITIGAÇÃO] Barreira física detectada: ', adj.adjacent_rivers) END,
                CONCAT('[LOGÍSTICA] Nível de Risco: ', v.logistics_risk_level)
            ]) AS x WHERE x IS NOT NULL
        ) as evidence_infrastructure_array,

      CASE
        -- ==========================================================
        -- 1. NÍVEL CRIME: INFRAÇÕES DE "VERDADE DE SOLO" (BLOQUEIO)
        -- ==========================================================
        WHEN v.slave_labor_match_confidence = 'HIGH' THEN 'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)'
        
        WHEN v.biome_name LIKE 'AMAZ%NIA' AND v.embargo_area_ha_raw >= 0.001 
             AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL OR v.embargo_date = '1900-01-01')
             THEN 'NOT ELIGIBLE - AMZ EMBARGO (CMN 5.081)'

        WHEN v.embargo_area_ha_raw > 0.1 
             AND (v.embargo_date >= v.forest_code_date OR v.embargo_date IS NULL OR v.embargo_date = '1900-01-01')
             THEN 'NOT ELIGIBLE - EMBARGO'

        WHEN EXISTS(SELECT 1 FROM UNNEST(v.embargo_sources) AS s WHERE s IN ('IBAMA', 'SEMA_MT', 'SIGA_MT', 'ICMBIO'))
             THEN 'NOT ELIGIBLE - EMBARGO (OFFICIAL SOURCE)'

        WHEN v.mapbiomas_deforested_ha_raw > LEAST(v.noise_threshold, 0.1) 
             AND v.mapbiomas_date >= v.forest_code_date
             THEN 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)'

        WHEN v.mapbiomas_deforested_ha_raw > 0.1 AND (v.mapbiomas_date IS NULL OR v.mapbiomas_date = '1900-01-01')
             THEN 'NOT ELIGIBLE - DEFORESTATION (MISSING DATE EVIDENCE)'

        WHEN v.eudr_deforested_ha > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - EUDR VIOLATION (POST-2020)'
        
        WHEN v.forensic_app_hidrica_ha > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - APP DEFORESTATION'

        -- ALINHAMENTO DE THRESHOLD: Se logística é CRITICAL, qualquer área > 0 bloqueia como estruturado
        WHEN ( (v.embargo_area_ha_raw > 0 OR v.mapbiomas_deforested_ha_raw > 0) AND v.logistics_risk_level = 'CRITICAL' )
             OR ( (v.embargo_area_ha_raw > 0.1 OR v.mapbiomas_deforested_ha_raw > 0.1) AND v.logistics_risk_level = 'HIGH' )
             THEN 'NOT ELIGIBLE - STRUCTURED ENVIRONMENTAL RISK'

        -- ==========================================================
        -- 2. NÍVEL TERRITORIAL: INVASÕES E FAIXAS DE DOMÍNIO (BLOQUEIO)
        -- ==========================================================
        WHEN v.forensic_ti_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_ti_identity IS FALSE THEN 'NOT ELIGIBLE - INDIGENOUS LAND'
        WHEN v.forensic_uc_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_uc_identity IS FALSE THEN 'NOT ELIGIBLE - CONSERVATION UNIT'
        WHEN v.forensic_quilombo_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_quilombo_identity IS FALSE THEN 'NOT ELIGIBLE - QUILOMBOLA (INVASION)'
        WHEN v.forensic_settlement_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_settlement_identity IS FALSE THEN 'NOT ELIGIBLE - SETTLEMENT (INVASION)'
        WHEN v.forensic_traditional_ha > {{ var('gis_noise_ha_threshold') }} AND v.is_traditional_identity IS FALSE THEN 'NOT ELIGIBLE - TRADITIONAL TERRITORY (INVASION)'
        
        WHEN v.road_fixed_ha > {{ var('gis_noise_ha_threshold') }} THEN 'NOT ELIGIBLE - INFRASTRUCTURE IN DEFORESTED AREA'

        -- ==========================================================
        -- 3. NÍVEL JURÍDICO: STATUS DO DOCUMENTO (BLOQUEIO)
        -- ==========================================================
        WHEN UPPER(TRIM(v.registration_status)) IN ('CANCELADO', 'SUSPENSO') 
             OR UPPER(TRIM(v.registration_status_geometry)) IN ('CANCELADO', 'SUSPENSO') 
             THEN 'NOT ELIGIBLE - CAR STATUS'

        WHEN v.is_missing_geometry = TRUE THEN 'NOT ELIGIBLE - INVALID GEOMETRY'

        -- ==========================================================
        -- 4. NÍVEL TÉCNICO: INCONSISTÊNCIAS (REVISÃO MANUAL)
        -- ==========================================================
        WHEN v.is_area_inconsistent THEN 'MANUAL_REVIEW - INCONSISTENT AREA'
        
        WHEN (ST_AREA(v.geometry) / 10000) > (v.area_ha * 1.50) THEN 'MANUAL_REVIEW_REQUIRED - POTENTIAL AREA FRAUD'

        WHEN UPPER(TRIM(v.registration_status)) IN ('PENDENTE', 'EM ANALISE') THEN 'MANUAL_REVIEW_REQUIRED - PENDING STATE ANALYSIS'

        WHEN v.count_artificial_water_bodies > 0 AND v.biome_name LIKE 'AMAZ%NIA' THEN 'MANUAL_REVIEW_REQUIRED - ARTIFICIAL WATER BODY DETECTED'

        -- ==========================================================
        -- 5. NÍVEL IDENTIDADE: PRODUTORES LEGÍTIMOS (ELEGÍVEL ESPECIAL)
        -- ==========================================================
        WHEN v.is_ti_identity IS TRUE THEN 'ELIGIBLE - INDIGENOUS PRODUCER'
        WHEN v.is_uc_identity IS TRUE THEN 'ELIGIBLE - CONSERVATION UNIT PRODUCER'
        WHEN v.is_quilombo_identity IS TRUE THEN 'ELIGIBLE - QUILOMBOLA PRODUCER'
        WHEN v.is_settlement_identity IS TRUE THEN 'ELIGIBLE - SETTLEMENT PRODUCER'
        WHEN v.is_traditional_identity IS TRUE THEN 'ELIGIBLE - TRADITIONAL PRODUCER'

        -- ==========================================================
        -- 6. NÍVEL RISCO INDIRETO: ADJACÊNCIA E AVISOS (WARNING)
        -- ==========================================================
        WHEN adj.max_adjacency_score > 0 AND adj.has_physical_barrier IS TRUE THEN 'ELIGIBLE - MITIGATED ADJACENCY RISK (PHYSICAL BARRIER)'
        
        WHEN adj.adjacency_risk_types LIKE '%LAUNDERING_RISK%' THEN 'WARNING - CRITICAL ADJACENCY RISK (LAUNDERING)'
        
        WHEN adj.max_adjacency_score >= 70 THEN 'WARNING - CRITICAL ADJACENCY RISK'
        
        WHEN adj.max_adjacency_score > 0 THEN 'WARNING - RISK BY ADJACENCY'

        WHEN v.rl_deficit_ha > 0.01 AND (v.is_small_holder IS TRUE OR v.solicitacao_adesao_pra = 'Sim') THEN 'CONDITIONAL - RL REGULARIZATION'
        
        WHEN v.rl_deficit_ha > 0.01 AND v.is_small_holder IS FALSE THEN 'NOT ELIGIBLE - RL DEFICIT'

        -- ==========================================================
        -- 7. NÍVEL LIMPO: ELEGÍVEL
        -- ==========================================================
        ELSE 'ELIGIBLE'
      END as final_eligibility_status,

        -- Cálculo do Passivo Financeiro Estimado
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

final_formatting AS (
    SELECT 
        *,
        -- 1. Nível de Confiança Geoespacial
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

        -- 2. Consolidação de evidências técnicas para o laudo
        ARRAY_TO_STRING(ARRAY_CONCAT(evidence_admin_array, evidence_social_array, evidence_environmental_array, evidence_infrastructure_array), ' | ') as technical_evidence_consolidated,
        
        -- 3. [CORREÇÃO] Lista de riscos internos (O que faltava para o seu SELECT final)
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

        -- 4. [CORREÇÃO] Avisos Históricos (Também faltava para o seu SELECT final)
        ARRAY_TO_STRING(ARRAY(
            SELECT x FROM UNNEST([
                CASE WHEN has_any_cancelled_embargo = TRUE THEN 'Embargo Cancelado' END,
                CASE WHEN defo_fixed_ha > noise_threshold AND mapbiomas_date < forest_code_date THEN 'Desmatamento Pré-2008' END,
                CASE WHEN rl_deficit_ha > 0.01 THEN 'Déficit de RL' END
            ]) AS x WHERE x IS NOT NULL
        ), ' | ') as historical_warnings,

        -- 5. Detalhamento do status para auditoria rápida
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
    -- 1. IDENTIDADE E DADOS BÁSICOS
    property_id, 
    property_alias, 
    property_identity_type, 
    area_ha, 
    area_ha as property_area_ha, 
    area_ha_original, -- [NOVO] Valor original declarado pelo produtor
    area_liquida_ha,
    fiscal_modules, 
    city, 
    city_data_source_origin, 
    uf_origem, 
    registration_status as car_status, 
    registration_status_geometry as car_status_spatial, 
    solicitacao_adesao_pra,
    biome_name, 
    geometry, 
    centroid, 
    car_bbox, 
    final_eligibility_status, 
    is_technically_blocked, 
    is_missing_geometry,
    geospatial_confidence_level,

    -- 2. PILAR JURÍDICO E ADMINISTRATIVO (EMBARGOS)
    embargo_fixed_ha as embargo_area_ha, 
    embargo_area_ha_raw, 
    has_any_active_embargo as is_embargo_active,
    (biome_name LIKE 'AMAZ%NIA' AND embargo_area_ha_raw >= 0.001) as is_cmn_5081_sensitive,
    embargo_offenders, 
    embargo_processes, -- [NOVO] Números dos processos
    embargo_tax_ids,   -- [NOVO] CPF/CNPJ dos autuados
    embargo_reported_areas, -- [NOVO] Área declarada no auto de infração
    ARRAY_TO_STRING(embargo_sources, ' | ') as embargo_sources_string,

    -- 3. PILAR DE INTEGRIDADE (ÁREA E GEOMETRIA)
    ROUND(ST_AREA(geometry) / 10000, 4) as area_geometria_ha,
    is_area_inconsistent,
    ((ST_AREA(geometry) / 10000) > (area_ha * 1.50)) as is_liability_uncertain,
    data_reliability_index,

    -- 4. PILAR AMBIENTAL (VERDADE DE SOLO & SATÉLITE)
    defo_fixed_ha as mapbiomas_deforested_ha, 
    mapbiomas_deforested_ha_raw, 
    eudr_deforested_ha, 
    mapbiomas_classes, -- [NOVO] Ex: Floresta, Formação Savânica
    mapbiomas_classes as deforestation_types,
    mapbiomas_report_links as official_reports_urls, 
    earliest_evidence_date as evidence_date_before, -- [NOVO] Data imagem ANTES
    latest_evidence_date as evidence_date_after,   -- [NOVO] Data imagem DEPOIS
    (eudr_deforested_ha > 0.01) as is_eudr_restricted,
    forest_code_date as reference_forest_code_date,

    -- 5. SOBREPOSIÇÕES TERRITORIAIS
    CASE WHEN is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity THEN 0 ELSE protected_area_fixed_ha END as protected_area_overlap_ha,
    (protected_area_fixed_ha > 0.01 AND NOT (is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity)) as is_protected_area_overlap,
    ti_name, uc_name, settlement_name, quilombo_name, traditional_name,
    area_rural_consolidada_ha, area_pousio_ha, area_uso_restrito_ha,
    car_on_car_overlap_pct, 
    CASE WHEN slave_labor_match_confidence = 'HIGH' THEN area_ha ELSE 0 END as slave_labor_overlap_ha,

    -- 6. PILAR DE INFRAESTRUTURA E LOGÍSTICA
    road_fixed_ha as road_overlap_ha,
    road_names, -- [NOVO] Nome das rodovias (Ex: BR-163)
    count_artificial_water_bodies, 
    artificial_water_details, -- [NOVO] Tipo de estrutura hídrica
    logistics_risk_level, 
    logistics_risk_score, 
    is_structured_environmental_risk,

    -- 7. PILAR DE ADJACÊNCIA (RISCO INDIRETO)
    adjacency_details, 
    max_adjacency_score, 
    adjacent_roads, 
    has_physical_barrier, 
    adjacent_rivers, -- [NOVO] Nome do rio (barreira física)
    critical_contact_point, -- [NOVO] Geometria do ponto de contato crítico

    -- 8. PASSIVO FINANCEIRO ESTIMADO
    COALESCE(defo_fixed_ha * fine_defo, 0) as liability_deforestation_brl,
    CASE WHEN is_small_holder OR solicitacao_adesao_pra = 'Sim' THEN 0 ELSE COALESCE(rl_deficit_ha * fine_rl, 0) END as liability_rl_brl,
    COALESCE(embargo_fixed_ha * fine_embargo, 0) as liability_embargo_brl,
    CASE WHEN is_settlement_identity OR is_traditional_identity OR is_quilombo_identity OR is_ti_identity OR is_uc_identity THEN 0 ELSE COALESCE(protected_area_fixed_ha * fine_protected, 0) END as liability_protected_areas_brl,
    CASE WHEN slave_labor_match_confidence = 'HIGH' THEN fine_slave ELSE 0 END as liability_social_brl,
    CASE WHEN (app_deforested_ha_forensic > 0.01 OR forensic_app_hidrica_ha > 0.01) THEN fine_app ELSE 0 END as liability_app_brl,
    estimated_financial_liability_brl,

    -- 9. EVIDÊNCIAS E RISCOS CONSOLIDADOS
    internal_risks_found,
    technical_evidence_consolidated as technical_evidence,
    historical_warnings,
    evidence_admin_array, 
    evidence_social_array, 
    evidence_environmental_array, 
    evidence_infrastructure_array,
    ARRAY_TO_STRING(evidence_admin_array, ' | ') as evidence_admin,
    ARRAY_TO_STRING(evidence_social_array, ' | ') as evidence_social,
    ARRAY_TO_STRING(evidence_environmental_array, ' | ') as evidence_environmental,
    ARRAY_TO_STRING(evidence_infrastructure_array, ' | ') as evidence_infrastructure,
    CONCAT('Status: ', final_eligibility_status, ' | Confiança: ', geospatial_confidence_level, ' | Índice: ', data_reliability_index) as forensic_summary,
    final_eligibility_status_detailed,

    -- 10. DATAS E MÉTRICAS DE CONTROLE
    COALESCE(mapbiomas_date, '1900-01-01') as mapbiomas_detection_date,
    COALESCE(embargo_date, '1900-01-01') as embargo_date,
    slave_labor_inclusion_date, 
    CURRENT_TIMESTAMP() as analyzed_at, 
    last_update as processed_at,
    ST_Y(centroid) as latitude, 
    ST_X(centroid) as longitude,
    mapbiomas_alert_ids, 
    mapbiomas_official_alert_ha as official_alert_area_ha,
    mapbiomas_official_ti_ha, 
    mapbiomas_official_quilombo_ha, 
    mapbiomas_official_settlement_ha,
    forensic_ti_ha, 
    forensic_quilombo_ha, 
    forensic_uc_ha, 
    forensic_settlement_ha, 
    forensic_traditional_ha,
    forensic_app_hidrica_ha, 
    forensic_app_declividade_ha,
    is_ti_identity, 
    is_uc_identity, 
    is_settlement_identity, 
    is_traditional_identity, 
    is_quilombo_identity,
    max_slope_degrees, 
    relief_classification, 
    rl_status, 
    rl_deficit_ha, 
    rl_balance_ha,
    producer_size_category, 
    is_small_holder, 
    fmp_ha,
    data_source_quality,
    ti_overlap_pct, 
    uc_overlap_pct, 
    settlement_overlap_pct, 
    traditional_overlap_pct

FROM final_formatting
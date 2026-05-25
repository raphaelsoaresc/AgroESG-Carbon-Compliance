{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['investment_rating', 'uf_origem', 'property_identity_type'],
    tags=['gold', 'investment', 'infrastructure', 'valuation']
) }}

WITH legal_params AS (
    -- 1. Parâmetros para cálculo de custo de reflorestamento e limites técnicos
    SELECT
        MAX(CASE WHEN parameter_name = 'fine_rl_deficit_per_ha' THEN CAST(value AS FLOAT64) END) as cost_reforestation_per_ha,
        MAX(CASE WHEN parameter_name = 'gis_noise_ha_threshold' THEN CAST(value AS FLOAT64) END) as noise_threshold
    FROM {{ ref('legal_parameters') }}
),

compliance_base AS (
    -- 2. O FUNIL: Filtro de entrada e definição do Multiplicador de Liquidez
    -- Removemos ativos "tóxicos" (Crimes humanitários e invasões de terras protegidas)
    SELECT 
        property_id,
        property_alias,
        uf_origem,
        city,
        area_ha,
        area_ha_original,
        area_rural_consolidada_ha,
        rl_status,
        rl_deficit_ha,
        rl_balance_ha,
        final_eligibility_status,
        property_identity_type,
        is_area_inconsistent,
        is_liability_uncertain,
        
        CASE 
            WHEN final_eligibility_status LIKE 'ELIGIBLE%' THEN 1.0
            WHEN final_eligibility_status LIKE 'WARNING%' THEN 0.8
            WHEN final_eligibility_status LIKE 'MANUAL_REVIEW%' THEN 0.7
            WHEN final_eligibility_status = 'NOT ELIGIBLE - RL DEFICIT' THEN 0.6
            WHEN final_eligibility_status = 'NOT ELIGIBLE - INVALID GEOMETRY' THEN 0.5
            WHEN final_eligibility_status LIKE 'NOT ELIGIBLE - AREA FRAUD%' THEN 0.4
            WHEN final_eligibility_status IN ('NOT ELIGIBLE - ACTIVE EMBARGO', 'NOT ELIGIBLE - DEFORESTATION (MAPBIOMAS)') THEN 0.2
            ELSE 0.0 
        END as liquidity_multiplier

    FROM {{ ref('fct_compliance_risk') }}
    WHERE final_eligibility_status NOT IN (
        'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)',
        'NOT ELIGIBLE - INDIGENOUS LAND',
        'NOT ELIGIBLE - CONSERVATION UNIT',
        'NOT ELIGIBLE - QUILOMBOLA (INVASION)',
        'NOT ELIGIBLE - SETTLEMENT (INVASION)',
        'NOT ELIGIBLE - TRADITIONAL TERRITORY (INVASION)'
    )
),

infra_unnested AS (
    -- 3. UNNEST da ponte de infraestrutura para extrair distâncias e descrições técnicas
    SELECT 
        property_id,
        b_json.feature_id,
        b_json.feature_type,
        b_json.feature_subtype,
        b_json.distance_m,
        b_json.evidence_description
    FROM {{ ref('int_compliance__infrastructure_bridge') }}
    CROSS JOIN UNNEST(logistics_evidence_json) as b_json
),

water_assets AS (
    -- 4. Ativos Hídricos: Extraindo Ordem do Rio e Reservatórios Artificiais
    SELECT 
        base.property_id,
        MAX(SAFE_CAST(REGEXP_EXTRACT(infra.evidence_description, r'ORDEM (\d+)') AS INT64)) as max_river_order,
        MAX(w.count_artificial_water_bodies) as reservoir_count
    FROM compliance_base base
    LEFT JOIN infra_unnested infra ON base.property_id = infra.property_id AND infra.feature_subtype = 'RIVER'
    LEFT JOIN {{ ref('int_property_water_anomalies') }} w ON base.property_id = w.property_id
    GROUP BY 1
),

energy_assets AS (
    -- 5. Ativos Energéticos: Conexão com o SIN e Proximidade da Rede
    SELECT 
        base.property_id,
        MAX(CASE WHEN infra.evidence_description LIKE '%Integrada ao SIN%' THEN 1 ELSE 0 END) as connected_to_sin,
        MIN(infra.distance_m) as dist_to_power_line
    FROM compliance_base base
    LEFT JOIN infra_unnested infra ON base.property_id = infra.property_id AND infra.feature_subtype = 'POWER_LINE'
    GROUP BY 1
),

logistics_assets AS (
    -- 6. Ativos Logísticos: Pavimentação de Rodovias e Pistas de Pouso
    SELECT 
        base.property_id,
        MAX(CASE WHEN infra.evidence_description LIKE '%PAVIMENTADO%' THEN 1 ELSE 0 END) as has_paved_access,
        MAX(CASE WHEN infra.evidence_description LIKE '%HOMOLOGADA%' THEN 1 ELSE 0 END) as has_certified_airstrip
    FROM compliance_base base
    LEFT JOIN infra_unnested infra ON base.property_id = infra.property_id 
    WHERE infra.feature_subtype IN ('ROAD', 'AIRSTRIP')
    GROUP BY 1
),

scoring_consolidation AS (
    -- 7. Consolidação do Score de Ativos (Raw Score) com Trava de Sanidade (LEAST 1.0)
    SELECT
        c.*,
        COALESCE(w.max_river_order, 0) as river_magnitude,
        COALESCE(w.reservoir_count, 0) as reservoirs,
        COALESCE(e.connected_to_sin, 0) as is_sin_connected,
        COALESCE(l.has_paved_access, 0) as paved_access_flag,
        COALESCE(l.has_certified_airstrip, 0) as airstrip_flag,
        
        -- TRAVA: Eficiência não pode ser maior que 100% (1.0)
        LEAST(SAFE_DIVIDE(COALESCE(c.area_rural_consolidada_ha, 0), NULLIF(c.area_ha, 0)), 1.0) as land_use_efficiency,

        -- CÁLCULO PONDERADO (0-100)
        (
            -- Solo (Max 30 pts)
            (LEAST(SAFE_DIVIDE(COALESCE(c.area_rural_consolidada_ha, 0), NULLIF(c.area_ha, 0)), 1.0) * 20) + 
            (CASE WHEN c.rl_status = 'SURPLUS' THEN 10 ELSE 0 END) +
            
            -- Água (Max 30 pts)
            (CASE WHEN w.max_river_order >= 4 THEN 20 WHEN w.max_river_order >= 1 THEN 10 ELSE 0 END) +
            (CASE WHEN w.reservoir_count > 0 THEN 10 ELSE 0 END) +
            
            -- Energia (Max 20 pts)
            (CASE WHEN e.connected_to_sin = 1 THEN 20 WHEN e.dist_to_power_line < 5000 THEN 10 ELSE 0 END) +
            
            -- Logística (Max 20 pts)
            (CASE WHEN l.has_paved_access = 1 THEN 10 ELSE 0 END) +
            (CASE WHEN l.has_certified_airstrip = 1 THEN 10 ELSE 0 END)
        ) as raw_asset_score

    FROM compliance_base c
    LEFT JOIN water_assets w ON c.property_id = w.property_id
    LEFT JOIN energy_assets e ON c.property_id = e.property_id
    LEFT JOIN logistics_assets l ON c.property_id = l.property_id
)

SELECT
    property_id,
    property_alias,
    uf_origem,
    city,
    property_identity_type,
    final_eligibility_status as compliance_status,
    
    -- Métricas de Solo
    area_ha,
    area_rural_consolidada_ha,
    ROUND(land_use_efficiency * 100, 2) as land_use_efficiency_pct,
    rl_balance_ha as green_asset_ha,
    
    -- KPIs de Infraestrutura
    river_magnitude,
    reservoirs as artificial_reservoirs_count,
    CASE WHEN is_sin_connected = 1 THEN TRUE ELSE FALSE END as has_stable_energy,
    CASE WHEN paved_access_flag = 1 THEN 'Asfalto' ELSE 'Terra/Sem Acesso' END as road_access_quality,
    
    -- Scoring e Rating Final (Garantindo escala 0-100)
    ROUND(raw_asset_score, 2) as raw_asset_score,
    ROUND(raw_asset_score * liquidity_multiplier, 2) as investment_score,
    
    CASE 
        WHEN (raw_asset_score * liquidity_multiplier) >= 80 THEN 'AAA - Premium Asset'
        WHEN (raw_asset_score * liquidity_multiplier) >= 60 THEN 'AA - High Productivity'
        WHEN (raw_asset_score * liquidity_multiplier) >= 40 THEN 'A - Operational'
        WHEN liquidity_multiplier < 0.5 AND raw_asset_score >= 60 THEN 'B - Turnaround Opportunity'
        WHEN liquidity_multiplier < 0.5 THEN 'C - High Risk / Distressed'
        ELSE 'D - Low Potential'
    END as investment_rating,

    -- Vetores de Monetização
    ARRAY_TO_STRING(ARRAY(
        SELECT x FROM UNNEST([
            CASE WHEN rl_status = 'SURPLUS' THEN 'Créditos de Carbono / CRA' END,
            CASE WHEN river_magnitude >= 4 THEN 'Potencial Irrigação (Pivô)' END,
            CASE WHEN is_sin_connected = 1 AND land_use_efficiency > 0.5 THEN 'Agroindústria / Armazenagem' END,
            CASE WHEN airstrip_flag = 1 THEN 'Logística Executiva' END
        ]) AS x WHERE x IS NOT NULL
    ), ' | ') as monetization_vectors,

    -- Custo Estimado de Regularização (Baseado no déficit de RL)
    ROUND(COALESCE(rl_deficit_ha * (SELECT cost_reforestation_per_ha FROM legal_params), 0), 2) as estimated_regularization_cost_brl,

    CURRENT_TIMESTAMP() as valuation_at

FROM scoring_consolidation
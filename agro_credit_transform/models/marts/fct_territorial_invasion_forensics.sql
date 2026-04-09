{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['estado', 'tipo_area_protegida', 'severidade_forense'],
    tags=['gold', 'forensic', 'invasion_analysis']
) }}

WITH invasoes_brutas AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        target_id,
        target_name as nome_area_protegida,
        target_type as tipo_area_protegida,
        target_area_ha as area_invasao_ha,
        overlap_pct as pct_invasao_sobre_fazenda,
        property_total_area_ha
    FROM {{ ref('int_compliance_forensic_shapes') }}
    WHERE target_type IN ('RECORTE_INVASAO_TI', 'RECORTE_INVASAO_QUILOMBO', 'RECORTE_INVASAO_UC', 'RECORTE_INVASAO_ASSENTAMENTO')
),

contexto_imovel AS (
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        property_alias,
        car_status,
        uf_origem as estado,
        city as municipio,
        is_ti_identity,
        is_uc_identity,
        is_settlement_identity,
        is_quilombo_identity,
        latitude,
        longitude
    FROM {{ ref('fct_compliance_risk') }}
),

logistica_nexo_causal AS (
    SELECT 
        property_id,
        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'AIRSTRIP' AND serves_forensic_violation IS TRUE
         LIMIT 1) as air_nexus,
        
        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'ROAD' AND serves_forensic_violation IS TRUE
         LIMIT 1) as road_nexus,

        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'RIVER' AND serves_forensic_violation IS TRUE
         LIMIT 1) as river_nexus
    FROM {{ ref('int_compliance__infrastructure_bridge') }}
)

SELECT 
    -- CHAVE PARA O TESTE UNIQUE/NOT NULL
    c.property_id as property_id,
    i.property_id as recibo_car_real,
    c.property_alias,
    i.nome_area_protegida,
    i.tipo_area_protegida,
    c.car_status,
    c.municipio,
    c.estado,
    ROUND(i.area_invasao_ha, 4) as area_invasao_ha,
    ROUND(i.property_total_area_ha, 2) as area_total_imovel_ha,
    ROUND(i.pct_invasao_sobre_fazenda, 2) as pct_invasao_sobre_fazenda,
    
    -- ⚖️ COLUNA BOOLEANA PARA O TESTE DO YAML
    COALESCE(l.air_nexus.evidence_description IS NOT NULL 
             OR l.road_nexus.evidence_description IS NOT NULL 
             OR l.river_nexus.evidence_description IS NOT NULL, FALSE) as nexo_causal_logistico,

    -- ⚖️ COLUNA TEXTUAL PARA O INVESTIGADOR
    CASE 
        WHEN l.air_nexus.evidence_description IS NOT NULL THEN '🚨 CRIME ESTRUTURADO: Pista de Pouso na Invasão'
        WHEN l.road_nexus.evidence_description IS NOT NULL THEN '🚜 CRIME ESTRUTURADO: Estrada de Acesso à Invasão'
        WHEN l.river_nexus.evidence_description IS NOT NULL THEN '🚢 LOGÍSTICA FLUVIAL: Escoamento por Rio Navegável'
        ELSE 'INVASÃO ISOLADA / SEM INFRAESTRUTURA DETECTADA'
    END as nexo_causal_investigativo,

    -- 🔍 PROVAS SEPARADAS
    COALESCE(l.air_nexus.evidence_description, 'Sem pistas no recorte') as prova_aerea,
    COALESCE(l.road_nexus.evidence_description, 'Sem estradas no recorte') as prova_rodoviaria,
    COALESCE(l.river_nexus.evidence_description, 'Sem rios navegáveis no recorte') as prova_fluvial,
    
    -- 🚨 SEVERIDADE FORENSE (AJUSTADA PARA BATER COM O TESTE)
    CASE 
        -- Se tem nexo (Ar, Terra OU Rio) e área > 1000, é ESTRUTURADA
        WHEN i.area_invasao_ha > 1000 AND (l.air_nexus.evidence_description IS NOT NULL 
                                          OR l.road_nexus.evidence_description IS NOT NULL 
                                          OR l.river_nexus.evidence_description IS NOT NULL) 
             THEN 'CRÍTICO: Mega-Invasão Estruturada (Nexo Logístico)'
        
        WHEN i.area_invasao_ha > 1000 AND c.car_status = 'ATIVO' THEN 'CRÍTICO: Mega-Invasão Ativa'
        WHEN i.pct_invasao_sobre_fazenda > 90 THEN 'CRÍTICO: CAR de Prateleira (Sobreposição Total)'
        WHEN l.air_nexus.evidence_description IS NOT NULL THEN 'ALTO: Invasão com Apoio Aéreo'
        WHEN i.area_invasao_ha > 1000 THEN 'ALTO: Mega-Invasão (Status Irregular)'
        ELSE 'ALTO: Invasão Parcial'
    END as severidade_forense,

    c.latitude,
    c.longitude,
    CONCAT('https://www.google.com/maps?q=', CAST(c.latitude AS STRING), ',', CAST(c.longitude AS STRING)) as link_google_maps,
    
    CURRENT_TIMESTAMP() as auditado_em

FROM invasoes_brutas i
JOIN contexto_imovel c ON i.property_id = c.property_id
LEFT JOIN logistica_nexo_causal l ON i.property_id = l.property_id

WHERE NOT (i.tipo_area_protegida = 'RECORTE_INVASAO_TI' AND c.is_ti_identity)
  AND NOT (i.tipo_area_protegida = 'RECORTE_INVASAO_UC' AND c.is_uc_identity)
  AND NOT (i.tipo_area_protegida = 'RECORTE_INVASAO_ASSENTAMENTO' AND c.is_settlement_identity)
  AND NOT (i.tipo_area_protegida = 'RECORTE_INVASAO_QUILOMBO' AND c.is_quilombo_identity)
  AND i.area_invasao_ha > 0.5001
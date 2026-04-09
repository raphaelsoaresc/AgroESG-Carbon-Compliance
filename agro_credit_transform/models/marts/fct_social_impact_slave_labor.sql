{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['uf_origem', 'inteligencia_aerea', 'risk_level'],
    tags=['gold', 'social_impact', 'forensic']
) }}

WITH targets AS (
    -- 1. Identificação dos infratores (Lista Suja MTE cruzada com SIGEF e CAR)
    SELECT 
        UPPER(TRIM(sigef_property_id)) as property_id,
        employer_name,
        'SIGEF_MATCH' as detection_source
    FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
    WHERE territorial_match_confidence = 'HIGH'

    UNION DISTINCT

    SELECT 
        UPPER(TRIM(car_property_id)) as property_id,
        employer_name,
        'SPATIAL_OVERLAP' as detection_source
    FROM {{ ref('int_compliance__final_spatial_check') }}
    WHERE risk_type = 'SOCIAL_RISK_SLAVE_LABOR'
),

compliance_context AS (
    -- 2. Contexto da Gold (Dados mestre da propriedade)
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        property_alias,
        city,
        uf_origem,
        area_ha,
        latitude,
        longitude,
        slave_labor_inclusion_date,
        internal_risks_found
    FROM {{ ref('fct_compliance_risk') }}
    WHERE internal_risks_found LIKE '%SOCIAL_CRITICAL%'
),

logistica_investigativa AS (
    -- 3. Inteligência de Infraestrutura: Extração de vetores de fuga e acesso
    SELECT 
        property_id,
        -- Busca a Pista de Pouso mais relevante
        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'AIRSTRIP' 
         ORDER BY distance_m ASC LIMIT 1) as air,
        
        -- Busca a Estrada mais relevante
        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'ROAD' 
         ORDER BY distance_m ASC LIMIT 1) as road,

        -- Busca o Rio mais relevante
        (SELECT AS STRUCT relationship_type, evidence_description, distance_m 
         FROM UNNEST(logistics_evidence_json) 
         WHERE feature_subtype = 'RIVER' 
         ORDER BY distance_m ASC LIMIT 1) as river
    FROM {{ ref('int_compliance__infrastructure_bridge') }}
),

clusters AS (
    -- 4. Densidade de casos no mesmo Grid de 11km (Inteligência de Cluster)
    SELECT 
        g.grid_id,
        COUNT(DISTINCT t.property_id) as densidade_casos
    FROM {{ ref('int_car_grid_mapping') }} g
    INNER JOIN targets t ON UPPER(TRIM(g.property_id)) = t.property_id
    GROUP BY 1
)

SELECT 
    -- CHAVE PRIMÁRIA (Obrigatória para os testes do dbt)
    c.property_id as property_id,
    
    -- IDENTIFICAÇÃO PARA O INVESTIGADOR
    c.property_id as recibo_car,
    c.property_alias,
    t.employer_name as infrator_identificado,
    c.city,
    c.uf_origem,
    c.area_ha,
    c.slave_labor_inclusion_date as data_lista_suja,

    -- ✈️ INTELIGÊNCIA AÉREA (Vetor de Fuga/Acesso Remoto)
    CASE 
        WHEN l.air.relationship_type = 'INTERNAL' THEN 
            IF(l.air.evidence_description LIKE '%Não Homologada%', '⚠️ PISTA INTERNA CLANDESTINA', '✅ PISTA INTERNA HOMOLOGADA')
        WHEN l.air.relationship_type = 'ADJACENT' THEN 
            CONCAT('PISTA PRÓXIMA (', CAST(ROUND(l.air.distance_m) AS STRING), 'm)')
        ELSE 'SEM PISTAS DETECTADAS'
    END as inteligencia_aerea,

    -- 🛣️ INTELIGÊNCIA RODOVIÁRIA (Logística de Resgate)
    CASE 
        WHEN l.road.relationship_type = 'INTERNAL' THEN 
            CONCAT('ESTRADA INTERNA: ', REGEXP_EXTRACT(l.road.evidence_description, r'via (.*?) \('))
        WHEN l.road.relationship_type = 'ADJACENT' THEN 
            CONCAT('ACESSO EXTERNO (', CAST(ROUND(l.road.distance_m) AS STRING), 'm)')
        ELSE 'ACESSO REMOTO / SEM ESTRADAS'
    END as inteligencia_rodoviaria,

    -- 🚢 INTELIGÊNCIA FLUVIAL (Confinamento/Escoamento)
    CASE 
        WHEN l.river.relationship_type = 'INTERNAL' THEN 
            IF(l.river.evidence_description LIKE '%NAVEGÁVEL%', '🚢 ROTA FLUVIAL NAVEGÁVEL', '🌊 RIO/CÓRREGO INTERNO')
        WHEN l.river.relationship_type = 'ADJACENT' THEN 
            CONCAT('RIO PRÓXIMO (', CAST(ROUND(l.river.distance_m) AS STRING), 'm)')
        ELSE 'SEM RIOS RELEVANTES'
    END as inteligencia_fluvial,

    -- 🚨 NÍVEL DE RISCO OPERACIONAL (Corrigido para bater com o YAML)
    CASE 
        WHEN l.air.relationship_type = 'INTERNAL' AND l.air.evidence_description LIKE '%Não Homologada%' THEN 'CRÍTICO: Cárcere com Pista Clandestina'
        WHEN l.air.relationship_type = 'INTERNAL' THEN 'CRÍTICO: Vetor de Fuga Aéreo Interno'
        WHEN l.river.relationship_type = 'INTERNAL' AND l.road.relationship_type IS NULL THEN 'CRÍTICO: Isolamento Geográfico (Acesso apenas por Rio)'
        WHEN l.road.relationship_type IS NULL THEN 'ALTO: Isolamento Geográfico Total'
        ELSE 'PADRÃO'
    END as risk_level,

    -- 🔍 DETALHAMENTO TÉCNICO (Para o Laudo Pericial)
    COALESCE(l.air.evidence_description, 'Nenhuma pista de pouso detectada num raio de 5km') as detalhe_pista_pouso,
    COALESCE(l.road.evidence_description, 'Nenhuma estrada oficial detectada num raio de 5km') as detalhe_malha_rodoviaria,
    COALESCE(l.river.evidence_description, 'Nenhum corpo d\'água relevante detectado num raio de 5km') as detalhe_hidrografia,

    -- 📍 INTELIGÊNCIA DE VIZINHANÇA
    COALESCE(clu.densidade_casos, 1) as casos_trabalho_escravo_no_mesmo_grid,

    -- 🗺️ DADOS GEOGRÁFICOS E GPS
    c.latitude,
    c.longitude,
    CONCAT('https://www.google.com/maps?q=', CAST(c.latitude AS STRING), ',', CAST(c.longitude AS STRING)) as link_google_maps,
    
    CURRENT_TIMESTAMP() as generated_at

FROM compliance_context c
INNER JOIN targets t ON c.property_id = t.property_id
LEFT JOIN logistica_investigativa l ON c.property_id = l.property_id
LEFT JOIN {{ ref('int_car_grid_mapping') }} g_map ON c.property_id = UPPER(TRIM(g_map.property_id))
LEFT JOIN clusters clu ON g_map.grid_id = clu.grid_id
{{ config(
    materialized='view',
    schema='agro_esg_marts',
    tags=['gold', 'spatial', 'mapa']
) }}

WITH wide_data AS (
    -- Pegamos as geometrias e os dados mestre da Fato
    -- Note que agora cruzamos com a fct_compliance_risk para pegar os detalhes periciais
    SELECT 
        f.property_id,
        f.final_eligibility_status,
        f.car_status,
        f.biome_name,
        f.property_area_ha,
        -- Detalhes periciais que você quer ver no mapa
        f.embargo_processes,
        f.embargo_offenders,
        f.embargo_area_ha as total_embargo_farm_ha,
        f.mapbiomas_detection_date,
        f.official_reports_urls,
        f.technical_evidence, -- MANTIDO/CONFIRMADO AQUI
        -- Geometrias vindas do Mart de Geometrias
        g.geom_car_total,
        g.geom_embargos,
        g.geom_desmatamento,
        g.geom_areas_protegidas,
        g.geom_conflito_app
    FROM {{ ref('fct_compliance_risk') }} f
    JOIN {{ ref('fct_compliance_geometries_mart') }} g ON f.property_id = g.property_id
)

-- 1. Camada de Contorno (A Fazenda)
SELECT 
    property_id,
    '1. LIMITE TOTAL' as layer_type,
    final_eligibility_status,
    ST_BOUNDARY(geom_car_total) as geometry,
    property_area_ha as area_ha,
    FORMAT("Status CAR: %s | Bioma: %s", car_status, biome_name) as info_detalhada,
    NULL as link_laudo,
    technical_evidence -- ADICIONADO PARA SAÍDA DA VIEW
FROM wide_data

UNION ALL

-- 2. Camada de Embargos
SELECT 
    property_id,
    '2. EMBARGO' as layer_type,
    final_eligibility_status,
    geom_embargos as geometry,
    total_embargo_farm_ha as area_ha,
    FORMAT("Processos: %s | Autuados: %s", embargo_processes, embargo_offenders) as info_detalhada,
    NULL as link_laudo,
    technical_evidence -- ADICIONADO PARA SAÍDA DA VIEW
FROM wide_data
WHERE geom_embargos IS NOT NULL

UNION ALL

-- 3. Camada de Desmatamento
SELECT 
    property_id,
    '3. DESMATAMENTO' as layer_type,
    final_eligibility_status,
    geom_desmatamento as geometry,
    NULL as area_ha, 
    FORMAT("Detectado em: %s | Evidência: MapBiomas", CAST(mapbiomas_detection_date AS STRING)) as info_detalhada,
    official_reports_urls as link_laudo,
    technical_evidence -- ADICIONADO PARA SAÍDA DA VIEW
FROM wide_data
WHERE geom_desmatamento IS NOT NULL

UNION ALL

-- 4. Camada de Áreas Protegidas
SELECT 
    property_id,
    '4. AREA PROTEGIDA' as layer_type,
    final_eligibility_status,
    geom_areas_protegidas as geometry,
    NULL as area_ha,
    "Sobreposição com TI/UC/Quilombo detectada via cruzamento espacial" as info_detalhada,
    NULL as link_laudo,
    technical_evidence -- ADICIONADO PARA SAÍDA DA VIEW
FROM wide_data
WHERE geom_areas_protegidas IS NOT NULL

UNION ALL

-- 5. Camada de Conflito em APP
SELECT 
    property_id,
    '5. CONFLITO APP' as layer_type,
    final_eligibility_status,
    geom_conflito_app as geometry,
    NULL as area_ha,
    "Vegetação degradada em zona de preservação permanente (NDVI < 0.4)" as info_detalhada,
    NULL as link_laudo,
    technical_evidence -- ADICIONADO PARA SAÍDA DA VIEW
FROM wide_data
WHERE geom_conflito_app IS NOT NULL
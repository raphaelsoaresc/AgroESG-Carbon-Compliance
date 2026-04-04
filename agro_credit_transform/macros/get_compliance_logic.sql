{% macro get_compliance_logic(state_code) %}

-- 1. Pega os dados declarados (Expandido com novas áreas)
WITH themes_pivoted AS (
    SELECT
        TRIM(property_id) as property_id,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_AVERBADA' THEN theme_area_ha ELSE 0 END) as rl_averbada_ha,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_PROPOSTA' THEN theme_area_ha ELSE 0 END) as rl_proposta_ha,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_APROVADA_NAO_AVERBADA' THEN theme_area_ha ELSE 0 END) as rl_aprovada_ha, -- NOVA
        SUM(CASE WHEN theme_name = 'APP' THEN theme_area_ha ELSE 0 END) as app_declared_ha,
        SUM(CASE WHEN theme_name = 'VEGETACAO_NATIVA' THEN theme_area_ha ELSE 0 END) as native_veg_ha,
        SUM(CASE WHEN theme_name = 'AREA_CONSOLIDADA' THEN theme_area_ha ELSE 0 END) as consolidated_area_ha,
        SUM(CASE WHEN theme_name = 'POUSIO' THEN theme_area_ha ELSE 0 END) as area_pousio_ha,             -- NOVA
        SUM(CASE WHEN theme_name = 'USO_RESTRITO' THEN theme_area_ha ELSE 0 END) as area_uso_restrito_ha, -- NOVA
        SUM(CASE WHEN theme_name = 'SERVIDAO_ADMINISTRATIVA' THEN theme_area_ha ELSE 0 END) as area_servidao_ha -- NOVA
    FROM {{ ref('stg_car_environmental_themes') }}
    GROUP BY 1
),

-- 2. Metadados do Imóvel (Nova CTE para trazer as colunas descritivas)
metadata AS (
    SELECT 
        property_id,
        uf,
        municipio,
        registration_status,
        registration_condition,
        tipo_imovel_rural as property_type, -- <--- CORRIGIDO
        solicitacao_adesao_pra,
        area_do_imovel,
        area_liquida
    FROM {{ ref('stg_car_owners') }}
),

-- 3. Geometrias do Estado Específico
properties AS (
    SELECT 
        TRIM(property_id) as property_id, 
        area_ha, 
        geometry_raw as geometry,
        ST_CENTROID(geometry_raw) as centroid,
        ingested_at
    FROM {{ ref('int_car_geometries') }}
    WHERE UPPER(TRIM(state)) = UPPER(TRIM('{{ state_code }}'))
    {% if is_incremental() %}
        AND ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

-- 4. Biome Match
biome_match AS (
    SELECT
        p.property_id,
        ref.restriction_name as biome_name,
        ref.legal_reserve_perc,
        ST_DISTANCE(p.centroid, ref.geometry) as dist_to_biome
    FROM properties p
    CROSS JOIN {{ ref('int_brazil_reference_geometries') }} ref
    WHERE ref.restriction_type = 'BIOME'
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY p.property_id 
        ORDER BY dist_to_biome ASC
    ) = 1
)

SELECT 
    p.property_id, 
    m.uf,                         -- NOVA
    m.municipio,                  -- NOVA
    m.registration_status,        -- NOVA
    m.registration_condition,     -- NOVA
    m.property_type,              -- NOVA
    m.solicitacao_adesao_pra,     -- NOVA
    p.area_ha as total_area_ha, 
    m.area_liquida as area_liquida_ha, -- NOVA
    b.biome_name,
    
    -- Cálculo de RL atualizado (Soma das 3 categorias)
    (COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) + COALESCE(t.rl_aprovada_ha, 0)) as total_rl_declared_ha,
    
    COALESCE(t.app_declared_ha, 0) as total_app_declared_ha,
    COALESCE(t.native_veg_ha, 0) as total_native_veg_ha,
    COALESCE(t.consolidated_area_ha, 0) as area_rural_consolidada_ha, -- NOVA
    COALESCE(t.area_pousio_ha, 0) as area_pousio_ha,                  -- NOVA
    COALESCE(t.area_uso_restrito_ha, 0) as area_uso_restrito_ha,      -- NOVA
    
    b.legal_reserve_perc,
    (p.area_ha * b.legal_reserve_perc) as required_rl_ha,
    
    -- Balanço de RL usando a nova soma total
    ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) + COALESCE(t.rl_aprovada_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) as rl_balance_ha,
    
    GREATEST(0, (p.area_ha * b.legal_reserve_perc) - (COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) + COALESCE(t.rl_aprovada_ha, 0))) as rl_deficit_ha,
    
    CASE 
        WHEN b.biome_name IS NULL THEN 'PENDENTE'
        WHEN ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) + COALESCE(t.rl_aprovada_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) > 0.1 THEN 'SURPLUS'
        WHEN ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) + COALESCE(t.rl_aprovada_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) >= -0.1 THEN 'REGULAR'
        ELSE 'DEFICIT'
    END as rl_status,
    
    p.ingested_at 
FROM properties p
LEFT JOIN metadata m ON p.property_id = m.property_id
LEFT JOIN themes_pivoted t ON p.property_id = t.property_id
LEFT JOIN biome_match b ON p.property_id = b.property_id

{% endmacro %}
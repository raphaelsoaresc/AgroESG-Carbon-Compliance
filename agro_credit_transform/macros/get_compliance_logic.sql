{% macro get_compliance_logic(state_code) %}

-- 1. Pega os dados declarados
WITH themes_pivoted AS (
    SELECT
        TRIM(property_id) as property_id,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_AVERBADA' THEN theme_area_ha ELSE 0 END) as rl_averbada_ha,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_PROPOSTA' THEN theme_area_ha ELSE 0 END) as rl_proposta_ha,
        SUM(CASE WHEN theme_name = 'APP' THEN theme_area_ha ELSE 0 END) as app_declared_ha,
        SUM(CASE WHEN theme_name = 'VEGETACAO_NATIVA' THEN theme_area_ha ELSE 0 END) as native_veg_ha,
        SUM(CASE WHEN theme_name = 'AREA_CONSOLIDADA' THEN theme_area_ha ELSE 0 END) as consolidated_area_ha
    FROM {{ ref('stg_car_environmental_themes') }}
    GROUP BY 1
),

-- 2. Geometrias do Estado Específico
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
        -- SÓ PROCESSA O QUE É NOVO: Velocidade máxima
        AND ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

-- 3. Biome Match Turbo
biome_match AS (
    SELECT
        p.property_id,
        ref.restriction_name as biome_name,
        ref.legal_reserve_perc,
        -- Calcula a distância para todos e o QUALIFY seleciona o mais perto
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
    p.property_id, p.area_ha as total_area_ha, b.biome_name,
    COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) as total_rl_declared_ha,
    COALESCE(t.app_declared_ha, 0) as total_app_declared_ha,
    COALESCE(t.native_veg_ha, 0) as total_native_veg_ha,
    b.legal_reserve_perc,
    (p.area_ha * b.legal_reserve_perc) as required_rl_ha,
    ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) as rl_balance_ha,
    GREATEST(0, (p.area_ha * b.legal_reserve_perc) - (COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0))) as rl_deficit_ha,
    CASE 
        WHEN b.biome_name IS NULL THEN 'PENDENTE'
        WHEN ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) > 0.1 THEN 'SURPLUS'
        WHEN ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) >= -0.1 THEN 'REGULAR'
        ELSE 'DEFICIT'
    END as rl_status,
    p.ingested_at -- Necessário para o controle incremental
FROM properties p
LEFT JOIN themes_pivoted t ON p.property_id = t.property_id
LEFT JOIN biome_match b ON p.property_id = b.property_id

{% endmacro %}
{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='property_id',
    tags=['car']
) }}

-- 1. Pega os dados declarados (Pivotando os temas para colunas)
WITH themes_pivoted AS (
    SELECT
        property_id,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_AVERBADA' THEN theme_area_ha ELSE 0 END) as rl_averbada_ha,
        SUM(CASE WHEN theme_name = 'RESERVA_LEGAL_PROPOSTA' THEN theme_area_ha ELSE 0 END) as rl_proposta_ha,
        SUM(CASE WHEN theme_name = 'APP' THEN theme_area_ha ELSE 0 END) as app_declared_ha,
        SUM(CASE WHEN theme_name = 'VEGETACAO_NATIVA' THEN theme_area_ha ELSE 0 END) as native_veg_ha,
        SUM(CASE WHEN theme_name = 'AREA_CONSOLIDADA' THEN theme_area_ha ELSE 0 END) as consolidated_area_ha
    FROM {{ ref('stg_car_environmental_themes') }}
    GROUP BY 1
),

-- 2. Pega a geometria do imóvel
properties AS (
    SELECT property_id, area_ha, centroid 
    FROM {{ ref('int_car_geometries') }}
),

-- 3. Descobre o Bioma usando sua tabela de referência
biome_match AS (
    SELECT
        p.property_id,
        ref.restriction_name as biome_name,
        ref.legal_reserve_perc, -- Já vem 0.80, 0.35 ou 0.20 da sua tabela!
        ST_DISTANCE(p.centroid, ref.geometry) as dist
    FROM properties p
    INNER JOIN {{ ref('int_brazil_reference_geometries') }} ref
        ON ref.restriction_type = 'BIOME'
        AND ST_DWITHIN(p.centroid, ref.geometry, 1000) -- Tolerância 1km
    QUALIFY ROW_NUMBER() OVER(PARTITION BY p.property_id ORDER BY dist ASC) = 1
)

SELECT 
    p.property_id,
    p.area_ha as total_area_ha,
    b.biome_name,
    
    -- Dados Declarados
    COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0) as total_rl_declared_ha,
    COALESCE(t.app_declared_ha, 0) as total_app_declared_ha,
    COALESCE(t.native_veg_ha, 0) as total_native_veg_ha,
    
    -- Regra de Compliance (Calculada)
    b.legal_reserve_perc,
    (p.area_ha * b.legal_reserve_perc) as required_rl_ha,
    
    -- Status de Compliance (Deficit/Superavit)
    ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) as rl_balance_ha,
    
    CASE 
        WHEN ((COALESCE(t.rl_averbada_ha, 0) + COALESCE(t.rl_proposta_ha, 0)) - (p.area_ha * b.legal_reserve_perc)) >= -0.1 THEN 'COMPLIANT'
        ELSE 'DEFICIT'
    END as rl_status

FROM properties p
LEFT JOIN themes_pivoted t ON p.property_id = t.property_id
LEFT JOIN biome_match b ON p.property_id = b.property_id
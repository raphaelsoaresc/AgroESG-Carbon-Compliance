{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id'],
    tags=['car']
) }}

WITH unioned AS (
    SELECT 
        property_id, 
        uf, 
        municipio, 
        registration_status, 
        registration_condition, 
        property_type, 
        solicitacao_adesao_pra,
        -- Substituído total_area_ha pelas colunas de Transparência Forense
        area_ha_original, 
        area_ha_ajustada, 
        is_area_inconsistent,
        area_liquida_ha,
        biome_name, 
        total_rl_declared_ha, 
        total_app_declared_ha, 
        total_native_veg_ha, 
        area_rural_consolidada_ha,
        area_pousio_ha,
        area_uso_restrito_ha,
        legal_reserve_perc, 
        required_rl_ha, 
        rl_balance_ha, 
        rl_deficit_ha, 
        rl_status, 
        ingested_at
    FROM {{ ref('int_car_compliance_mt') }}
    
    UNION ALL
    
    SELECT 
        property_id, uf, municipio, registration_status, registration_condition, 
        property_type, solicitacao_adesao_pra, 
        area_ha_original, area_ha_ajustada, is_area_inconsistent,
        area_liquida_ha, biome_name, total_rl_declared_ha, total_app_declared_ha, 
        total_native_veg_ha, area_rural_consolidada_ha, area_pousio_ha, area_uso_restrito_ha,
        legal_reserve_perc, required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_pa') }}
    
    UNION ALL
    
    SELECT 
        property_id, uf, municipio, registration_status, registration_condition, 
        property_type, solicitacao_adesao_pra, 
        area_ha_original, area_ha_ajustada, is_area_inconsistent,
        area_liquida_ha, biome_name, total_rl_declared_ha, total_app_declared_ha, 
        total_native_veg_ha, area_rural_consolidada_ha, area_pousio_ha, area_uso_restrito_ha,
        legal_reserve_perc, required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_ro') }}

    UNION ALL
    
    SELECT 
        property_id, uf, municipio, registration_status, registration_condition, 
        property_type, solicitacao_adesao_pra, 
        area_ha_original, area_ha_ajustada, is_area_inconsistent,
        area_liquida_ha, biome_name, total_rl_declared_ha, total_app_declared_ha, 
        total_native_veg_ha, area_rural_consolidada_ha, area_pousio_ha, area_uso_restrito_ha,
        legal_reserve_perc, required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_am') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() as consolidated_at
FROM unioned
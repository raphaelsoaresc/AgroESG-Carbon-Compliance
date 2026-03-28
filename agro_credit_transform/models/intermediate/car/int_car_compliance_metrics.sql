{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['property_id']
) }}

WITH unioned AS (
    SELECT 
        property_id, total_area_ha, biome_name, total_rl_declared_ha, 
        total_app_declared_ha, total_native_veg_ha, legal_reserve_perc, 
        required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_mt') }}
    
    UNION ALL
    
    SELECT 
        property_id, total_area_ha, biome_name, total_rl_declared_ha, 
        total_app_declared_ha, total_native_veg_ha, legal_reserve_perc, 
        required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_pa') }}
    
    UNION ALL
    
    SELECT 
        property_id, total_area_ha, biome_name, total_rl_declared_ha, 
        total_app_declared_ha, total_native_veg_ha, legal_reserve_perc, 
        required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_ro') }}


    UNION ALL
    
    SELECT 
        property_id, total_area_ha, biome_name, total_rl_declared_ha, 
        total_app_declared_ha, total_native_veg_ha, legal_reserve_perc, 
        required_rl_ha, rl_balance_ha, rl_deficit_ha, rl_status, ingested_at
    FROM {{ ref('int_car_compliance_am') }}
)

SELECT 
    property_id,
    total_area_ha,
    biome_name,
    total_rl_declared_ha,
    total_app_declared_ha,
    total_native_veg_ha,
    legal_reserve_perc,
    required_rl_ha,
    rl_balance_ha,
    rl_deficit_ha,
    rl_status,
    ingested_at,
    CURRENT_TIMESTAMP() as consolidated_at
FROM unioned
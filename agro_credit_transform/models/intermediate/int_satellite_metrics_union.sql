{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    unique_key='property_id'
) }}

WITH topo AS (
    SELECT * FROM {{ ref('stg_property_topography') }}
),

app AS (
    SELECT * FROM {{ ref('stg_property_app_ndvi') }}
)

SELECT
    t.property_id,
    t.grid_id,
    -- Dados da Fazenda Toda
    t.min_elevation_meters,
    t.max_elevation_meters,
    t.avg_slope_degrees,
    t.max_slope_degrees,
    t.ndvi_mean as general_ndvi_mean,
    t.vegetation_state as general_vegetation_state,
    t.is_high_slope_risk,
    
    -- Dados Específicos da APP (Hídrica)
    COALESCE(a.app_ndvi_mean, 0) as app_ndvi_mean,
    COALESCE(a.app_vegetation_status, 'NO_APP_DATA') as app_vegetation_status,
    COALESCE(a.is_app_violation_risk, FALSE) as is_app_violation_risk,
    
    -- Cruzamento de Inteligência
    CASE 
        WHEN t.ndvi_mean > 0.6 AND a.app_ndvi_mean < 0.4 THEN TRUE 
        ELSE FALSE 
    END as alert_selective_deforestation_in_app, -- Fazenda verde, mas rio pelado

    GREATEST(t.processed_at, COALESCE(a.processed_at, t.processed_at)) as last_satellite_update

FROM topo t
LEFT JOIN app a ON t.property_id = a.property_id
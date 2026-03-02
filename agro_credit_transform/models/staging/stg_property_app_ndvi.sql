{{ config(
    materialized='incremental',
    schema='agro_esg_staging',
    unique_key='property_id',
    tags=['satellite', 'app', 'compliance']
) }}

WITH raw_app_data AS (
    SELECT
        CAST(property_id AS STRING) as property_id,
        CAST(grid_id AS STRING) as grid_id,
        
        -- NDVI específico da zona de APP (margem do rio)
        CAST(app_ndvi_mean AS FLOAT64) as app_ndvi_mean,
        CAST(app_ndvi_min AS FLOAT64) as app_ndvi_min,
        CAST(app_ndvi_max AS FLOAT64) as app_ndvi_max,
        
        -- Metadados de Auditoria
        CAST(processed_at AS TIMESTAMP) as processed_at,
        CAST(analysis_start_date AS DATE) as analysis_start_date,
        CAST(analysis_end_date AS DATE) as analysis_end_date,
        'Sentinel-2 (Surgical APP Audit)' as data_source
    FROM {{ source('raw_data', 'raw_ee_app_ndvi') }} 

    {% if is_incremental() %}
        WHERE CAST(processed_at AS TIMESTAMP) > (SELECT MAX(processed_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    *,
    -- Lógica de saúde da APP
    CASE 
        WHEN app_ndvi_mean IS NULL THEN 'CLOUD_COVERED'
        WHEN app_ndvi_mean < 0.4 THEN 'DEGRADED_APP' -- Limiar de APP costuma ser maior que pasto
        WHEN app_ndvi_mean < 0.6 THEN 'RECOVERING_APP'
        ELSE 'HEALTHY_APP'
    END as app_vegetation_status,

    CASE 
        WHEN app_ndvi_mean < 0.4 THEN TRUE 
        ELSE FALSE 
    END as is_app_violation_risk

FROM raw_app_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY processed_at DESC) = 1
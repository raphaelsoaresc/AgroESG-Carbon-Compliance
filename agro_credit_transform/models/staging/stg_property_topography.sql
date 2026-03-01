{{ config(
    materialized='incremental',
    schema='agro_esg_staging',
    unique_key='property_id',
    cluster_by=['property_id', 'grid_id'],
    tags=['satellite', 'topography', 'ndvi', 'compliance']
) }}

WITH raw_satellite_data AS (
    SELECT
        CAST(property_id AS STRING) as property_id,
        CAST(grid_id AS STRING) as grid_id,
        
        -- Dados de Relevo (SRTM)
        CAST(elevation_min AS FLOAT64) as min_elevation_meters,
        CAST(elevation_max AS FLOAT64) as max_elevation_meters,
        CAST(slope_degrees_mean AS FLOAT64) as avg_slope_degrees,
        CAST(slope_degrees_max AS FLOAT64) as max_slope_degrees,
        
        -- Dados de NDVI (Sentinel-2)
        CAST(ndvi_mean AS FLOAT64) as ndvi_mean,
        CAST(ndvi_max AS FLOAT64) as ndvi_max,
        
        -- Metadados
        CAST(processed_at AS TIMESTAMP) as processed_at,
        'SRTM + Sentinel-2' as data_source
    FROM {{ source('raw_data', 'raw_ee_topography') }} 

    {% if is_incremental() %}
        -- Filtro incremental para performance
        WHERE CAST(processed_at AS TIMESTAMP) > (SELECT MAX(processed_at) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    
    -- 1. FLAG DE RISCO DE DECLIVIDADE (APP - Código Florestal)
    CASE 
        WHEN max_slope_degrees > 45 THEN TRUE 
        ELSE FALSE 
    END as is_high_slope_risk,

    -- 2. ESTADO DA VEGETAÇÃO (Descritivo + Tratamento de Nuvens do Código 1)
    CASE 
        WHEN ndvi_mean IS NULL THEN 'CLOUD_COVERED'
        WHEN ndvi_mean < 0.3 THEN 'LOW_VEGETATION'
        ELSE 'HEALTHY_VEGETATION'
    END as vegetation_state,

    -- 3. FLAG DE RISCO DE VEGETAÇÃO (Booleano para filtros rápidos)
    CASE 
        WHEN ndvi_mean < 0.3 THEN TRUE 
        ELSE FALSE 
    END as is_low_vegetation_risk,

    -- 4. ALERTA CRÍTICO (Cruzamento de Relevo + Vegetação do Código 2)
    CASE 
        WHEN max_slope_degrees > 45 AND ndvi_mean < 0.3 THEN TRUE 
        ELSE FALSE 
    END as is_critical_environmental_violation,

    -- 5. CLASSIFICAÇÃO DE RELEVO (Padrão Embrapa do Código 2)
    CASE 
        WHEN avg_slope_degrees <= 3 THEN 'PLANO'
        WHEN avg_slope_degrees <= 8 THEN 'SUAVE ONDULADO'
        WHEN avg_slope_degrees <= 20 THEN 'ONDULADO'
        WHEN avg_slope_degrees <= 45 THEN 'FORTE ONDULADO'
        ELSE 'MONTANHOSO/ESCARPADO'
    END as relief_classification

FROM raw_satellite_data
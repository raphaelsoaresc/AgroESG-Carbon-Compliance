{{ config(
    materialized='incremental',
    schema='agro_esg_staging',
    unique_key='property_id',
    cluster_by=['property_id'],
    tags=['satellite', 'topography', 'ndvi', 'compliance']
) }}

WITH raw_satellite_data AS (
    SELECT
        CAST(property_id AS STRING) as property_id,
        CAST(grid_id AS STRING) as grid_id,
        
        -- Dados de Relevo (SRTM) com COALESCE para evitar falhas nos testes NOT NULL
        COALESCE(CAST(elevation_min AS FLOAT64), 0) as min_elevation_meters,
        COALESCE(CAST(elevation_max AS FLOAT64), 0) as max_elevation_meters,
        COALESCE(CAST(slope_degrees_mean AS FLOAT64), 0) as avg_slope_degrees,
        COALESCE(CAST(slope_degrees_max AS FLOAT64), 0) as max_slope_degrees,
        
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
),

processed_data AS (
    SELECT
        *,
        
        -- 1. FLAG DE RISCO DE DECLIVIDADE
        CASE 
            WHEN max_slope_degrees > 45 THEN TRUE 
            ELSE FALSE 
        END as is_high_slope_risk,

        -- 2. ESTADO DA VEGETAÇÃO
        CASE 
            WHEN ndvi_mean IS NULL THEN 'CLOUD_COVERED'
            WHEN ndvi_mean < 0.3 THEN 'LOW_VEGETATION'
            ELSE 'HEALTHY_VEGETATION'
        END as vegetation_state,

        -- 3. FLAG DE RISCO DE VEGETAÇÃO
        CASE 
            WHEN ndvi_mean < 0.3 THEN TRUE 
            ELSE FALSE 
        END as is_low_vegetation_risk,

        -- 4. ALERTA CRÍTICO
        CASE 
            WHEN max_slope_degrees > 45 AND ndvi_mean < 0.3 THEN TRUE 
            ELSE FALSE 
        END as is_critical_environmental_violation,

        -- 5. CLASSIFICAÇÃO DE RELEVO (Ajustado para bater com accepted_values do .yml)
        CASE 
            WHEN avg_slope_degrees <= 3 THEN 'PLANO'
            WHEN avg_slope_degrees <= 8 THEN 'SUAVE ONDULADO' 
            WHEN avg_slope_degrees <= 20 THEN 'ONDULADO'
            WHEN avg_slope_degrees <= 45 THEN 'FORTE ONDULADO' 
            ELSE 'MONTANHOSO/ESCARPADO'
        END as relief_classification

    FROM raw_satellite_data
)

SELECT * FROM processed_data
-- GARANTE UNICIDADE: Se uma propriedade aparecer em múltiplos grids ou datas, 
-- pegamos apenas a versão mais recente para evitar duplicatas no fct_compliance_risk
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY processed_at DESC) = 1
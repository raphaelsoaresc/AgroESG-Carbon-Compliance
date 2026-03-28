{{ config(materialized='view', schema='agro_esg_staging') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'raw_ee_topography') }}
    WHERE property_id IS NOT NULL
)

SELECT
    CAST(property_id AS STRING) as property_id,
    CAST(grid_id AS STRING) as grid_id,
    CAST(elevation_max AS FLOAT64) as max_elevation_meters,
    CAST(slope_degrees_max AS FLOAT64) as max_slope_degrees,
    CAST(slope_degrees_mean AS FLOAT64) as avg_slope_degrees,
    CAST(ndvi_mean AS FLOAT64) as general_ndvi_mean,
    
    CASE 
        WHEN CAST(slope_degrees_mean AS FLOAT64) <= 3 THEN 'PLANO'
        WHEN CAST(slope_degrees_mean AS FLOAT64) <= 8 THEN 'SUAVE ONDULADO' 
        WHEN CAST(slope_degrees_mean AS FLOAT64) <= 20 THEN 'ONDULADO'
        WHEN CAST(slope_degrees_mean AS FLOAT64) <= 45 THEN 'FORTE ONDULADO'
        WHEN CAST(slope_degrees_mean AS FLOAT64) <= 75 THEN 'MONTANHOSO'
        ELSE 'ESCARPADO'
    END as relief_classification,

    -- 🟢 CORREÇÃO: O BigQuery entende o formato ISO da DAG automaticamente com CAST
    CAST(processed_at AS TIMESTAMP) as processed_at,
    SAFE.PARSE_DATE('%Y-%m-%d', ndvi_start_date) as analysis_start_date,
    SAFE.PARSE_DATE('%Y-%m-%d', ndvi_end_date) as analysis_end_date
FROM source_data
-- 🛡️ FILTRO DE SANIDADE: Ignora registros fantasmas da DAG que não têm dados reais
WHERE slope_degrees_mean IS NOT NULL 
QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY processed_at DESC) = 1
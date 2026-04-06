{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['state', 'property_id'],
    tags=['car']
) }}

WITH staging_data AS (
    SELECT 
        property_id,
        -- Novas colunas vindas da staging refatorada
        area_ha_original,
        area_ha_ajustada,
        is_area_inconsistent,
        fiscal_modules,
        status_code,
        condition_desc,
        property_type,
        city,
        state,
        uf_origem,
        geometry_wkt,
        ingested_at
    FROM {{ ref('stg_car_properties') }}
),

spatial_processing AS (
    SELECT
        * EXCEPT(geometry_wkt),
        -- Converte WKT para Geography com correção topológica
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry_raw
    FROM staging_data
    WHERE geometry_wkt IS NOT NULL
),

final_cleaning AS (
    SELECT 
        *,
        -- Geometria Simplificada para visualização (20m de tolerância)
        ST_SIMPLIFY(geometry_raw, 20) as geometry_simplified,
        -- Centróide para análise de bioma e pins de mapa
        ST_CENTROID(geometry_raw) as centroid,
        -- Cálculo da Bounding Box para Joins espaciais performáticos
        ST_BOUNDINGBOX(geometry_raw) as car_bbox,
        -- Deduplicação garantindo a versão mais recente
        ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) as rn
    FROM spatial_processing
    WHERE geometry_raw IS NOT NULL
        AND ST_GEOMETRYTYPE(geometry_raw) IN ('ST_Polygon', 'ST_MultiPolygon')
)

SELECT 
    property_id,
    area_ha_original,
    area_ha_ajustada,
    is_area_inconsistent,
    fiscal_modules,
    status_code,
    condition_desc,
    property_type,
    city,
    state,
    uf_origem,
    ingested_at,
    
    -- Geometrias para diferentes usos
    geometry_raw,
    geometry_raw as geometry, -- Alias para compatibilidade com modelos legados
    geometry_simplified,
    centroid,
    car_bbox

FROM final_cleaning 
WHERE rn = 1
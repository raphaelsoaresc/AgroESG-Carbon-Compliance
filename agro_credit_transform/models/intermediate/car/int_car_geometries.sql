{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['state', 'property_id'],
    tags=['car']
) }}

WITH staging_data AS (
    SELECT 
        property_id,
        area_ha,
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
        property_id,
        area_ha,
        fiscal_modules,
        status_code,
        condition_desc,
        property_type,
        city,
        state,
        uf_origem,
        ingested_at,
        -- Converte WKT para Geography com correção topológica
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry_raw
    FROM staging_data
    WHERE geometry_wkt IS NOT NULL
),

final_cleaning AS (
    SELECT 
        property_id,
        area_ha,
        fiscal_modules,
        status_code,
        condition_desc,
        property_type,
        city,
        state,
        uf_origem,
        ingested_at,
        geometry_raw,
        -- Geometria Simplificada para visualização (20m de tolerância)
        ST_SIMPLIFY(geometry_raw, 20) as geometry_simplified,
        -- Centróide para análise de bioma e pins de mapa
        ST_CENTROID(geometry_raw) as centroid,
        -- 🟢 O PULO DO GATO: Cálculo da Bounding Box para o Join de vizinhança
        ST_BOUNDINGBOX(geometry_raw) as car_bbox,
        -- Deduplicação garantindo a versão mais recente
        ROW_NUMBER() OVER(PARTITION BY property_id ORDER BY ingested_at DESC) as rn
    FROM spatial_processing
    WHERE geometry_raw IS NOT NULL
        AND ST_GEOMETRYTYPE(geometry_raw) IN ('ST_Polygon', 'ST_MultiPolygon')
)

SELECT 
    property_id,
    area_ha,
    fiscal_modules,
    status_code,
    condition_desc,
    property_type,
    city,
    state,
    uf_origem,
    ingested_at,
    
    -- 1. Geometria Bruta (Uso em Cálculos de Passivo/ART)
    geometry_raw,
    
    -- 2. Geometria de Compatibilidade
    geometry_raw as geometry,
    
    -- 3. Geometria de Visualização
    geometry_simplified,
    
    -- 4. Ponto de Referência
    centroid,

    -- 5. 🟢 EXPORTANDO A BBOX: Sem isso o Mart não enxerga a coluna!
    car_bbox

FROM final_cleaning 
WHERE rn = 1
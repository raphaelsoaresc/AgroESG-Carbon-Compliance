{{ config(
    materialized='table',
    schema='agro_esg_marts',
    cluster_by=['city_id']
) }}

-- 1. Preparamos o SIGEF
WITH sigef AS (
    SELECT 
        property_id,
        property_name,
        city_id,
        geometry,
        ST_AREA(geometry) / 10000 as property_area_ha
    FROM {{ ref('int_sigef_geometries') }}
),

-- 2. Preparamos o IBAMA (Usando as colunas que realmente existem no int_ibama_geometries)
ibama AS (
    SELECT 
        embargo_date,
        form_status,
        geometry
    FROM {{ ref('int_ibama_geometries') }}
    WHERE is_active_embargo = TRUE 
    AND geometry IS NOT NULL
),

-- 3. IDENTIFICAMOS OS MATCHES
matches AS (
    SELECT
        s.property_id,
        i.form_status as embargo_status,
        i.embargo_date,
        -- SÓ calculamos a área de interseção para quem passou no filtro ST_INTERSECTS
        ST_AREA(ST_INTERSECTION(ST_SNAPTOGRID(s.geometry, 0.0001), ST_SNAPTOGRID(i.geometry, 0.0001))) / 10000 as overlap_area_ha
    FROM sigef s
    INNER JOIN ibama i ON ST_INTERSECTS(s.geometry, i.geometry)
),

-- 4. UNIMOS TUDO
spatial_join AS (
    SELECT
        s.*,
        b.biome_type,
        m.embargo_status,
        m.embargo_date,
        COALESCE(m.overlap_area_ha, 0) as overlap_area_ha
    FROM sigef s
    LEFT JOIN matches m ON s.property_id = m.property_id
    LEFT JOIN {{ ref('mt_cities_biomes') }} b ON CAST(s.city_id AS INT64) = CAST(b.city_id AS INT64)
),

enriched_logic AS (
    SELECT
        *,
        SAFE_DIVIDE(overlap_area_ha, property_area_ha) * 100 as overlap_percentage,
        -- Removida a geom_last_modified_date que não existia
        embargo_date as reference_date
    FROM spatial_join
),

final_verdict AS (
    SELECT
        *,
        CASE 
            WHEN overlap_area_ha < 0.001 OR overlap_area_ha IS NULL THEN 'ELIGIBLE'
            WHEN overlap_area_ha < 0.1 THEN 'ELIGIBLE - NEGLIGIBLE OVERLAP'
            WHEN reference_date IS NULL THEN 'NOT ELIGIBLE - INCONCLUSIVE DATA'
            WHEN reference_date >= '2008-07-22' THEN 
                CASE 
                    WHEN biome_type = 'AMAZONIA' THEN 'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION'
                    ELSE 'NOT ELIGIBLE - POST-2008 VIOLATION'
                END
            WHEN reference_date < '2008-07-22' THEN 'ELIGIBLE W/ MONITORING (CONSOLIDATED)'
            ELSE 'UNDER ANALYSIS'
        END as eligibility_status
    FROM enriched_logic
)

SELECT * FROM final_verdict

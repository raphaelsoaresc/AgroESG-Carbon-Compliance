{{ config(
    materialized='table',
    schema='agro_esg_marts'
) }}

WITH sigef AS (
    SELECT 
        *,
        ST_AREA(geometry) / 10000 as property_area_ha
    FROM {{ ref('int_sigef_geometries') }}
),

ibama AS (
    SELECT * FROM {{ ref('int_ibama_geometries') }}
),

biomes AS (
    SELECT * FROM {{ ref('mt_cities_biomes') }}
),

-- Realizamos o cruzamento espacial
spatial_join AS (
    SELECT
        s.property_id,
        s.property_name,
        s.property_area_ha,
        s.city_id,
        b.biome_type,
        b.legal_reserve_perc,
        i.form_status as embargo_status,
        i.embargo_date,
        i.geom_last_modified_date,
        i.is_active_embargo,
        
        -- CÁLCULO DE PRECISÃO TOTAL (Usa geometrias originais)
        CASE 
            WHEN i.geometry IS NOT NULL THEN ST_AREA(ST_INTERSECTION(s.geometry, i.geometry)) / 10000 
            ELSE 0 
        END as overlap_area_ha,
        
        s.geometry
    FROM sigef s
    LEFT JOIN biomes b ON CAST(s.city_id AS INT64) = CAST(b.city_id AS INT64)
    
    -- FILTRO DE PERFORMANCE (Simplificação de 60m para o Join)
    LEFT JOIN ibama i ON ST_INTERSECTS(ST_SIMPLIFY(s.geometry, 60), ST_SIMPLIFY(i.geometry, 60))
),

enriched_logic AS (
    SELECT
        *,
        SAFE_DIVIDE(overlap_area_ha, property_area_ha) * 100 as overlap_percentage,
        COALESCE(embargo_date, geom_last_modified_date) as reference_date,
        CASE 
            WHEN embargo_date IS NOT NULL THEN 'OFFICIAL'
            WHEN geom_last_modified_date IS NOT NULL THEN 'ESTIMATED'
            ELSE 'INCONCLUSIVE'
        END as date_quality
    FROM spatial_join
),

final_verdict AS (
    SELECT
        *,
        CASE 
            -- 1. Trava de Ruído de Simplificação (Menos de 100m2 ignoramos)
            WHEN overlap_area_ha < 0.01 OR overlap_area_ha IS NULL THEN 'ELIGIBLE'

            -- 2. Tolerância de Negócio (Entre 0.01 e 0.1 ha)
            WHEN overlap_area_ha < 0.1 THEN 'ELIGIBLE - NEGLIGIBLE OVERLAP'

            -- 3. Incerteza de dados (Bloqueio por falta de rastreabilidade)
            WHEN date_quality = 'INCONCLUSIVE' THEN 'NOT ELIGIBLE - INCONCLUSIVE DATA'

            -- 4. Violação Pós-2008 (Marco Legal do Código Florestal)
            WHEN reference_date >= '2008-07-22' THEN 
                CASE 
                    WHEN biome_type = 'AMAZONIA' THEN 'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION'
                    ELSE 'NOT ELIGIBLE - POST-2008 VIOLATION'
                END

            -- 5. Área Consolidada (Embargos antigos)
            WHEN reference_date < '2008-07-22' THEN 'ELIGIBLE W/ MONITORING (CONSOLIDATED)'

            ELSE 'UNDER ANALYSIS'
        END as eligibility_status
    FROM enriched_logic
)

SELECT * FROM final_verdict
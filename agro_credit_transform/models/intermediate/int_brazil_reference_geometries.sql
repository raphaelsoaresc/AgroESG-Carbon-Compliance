{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['restriction_type', 'priority_level']
) }}

WITH biomes AS (
    SELECT 
        CONCAT('BIOME_', CAST(biome_code AS STRING)) as restriction_id, 
        biome_name as restriction_name,
        'BIOME' as restriction_type, 
        biome_name as restriction_subtype,
        CASE 
            WHEN biome_name = 'AMAZÔNIA' THEN 0.80 
            WHEN biome_name = 'CERRADO' THEN 0.35 
            ELSE 0.20 
        END as legal_reserve_perc,
        FALSE as is_hard_block, 
        2 as priority_level,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_ibge_biomes') }}
),

indigenous AS (
    SELECT 
        CONCAT('FUNAI_', CAST(territory_code AS STRING)) as restriction_id, 
        territory_name as restriction_name,
        'INDIGENOUS_LAND' as restriction_type, 
        stage_name as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_funai_indigenous_lands') }}
),

-- NOVA HIDROGRAFIA (RIOS) - BARREIRA FÍSICA E APP
app_rivers AS (
    SELECT 
        CONCAT('ANA_', CAST(basin_code AS STRING)) as restriction_id, 
        CONCAT('APP RIO - ORDEM ', CAST(river_order AS STRING)) as restriction_name,
        'APP_ZONE' as restriction_type, 
        'RIVER' as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_ana__rios_app') }}
),

-- NOVA HIDROGRAFIA (MASSAS D'ÁGUA) - BARREIRA FÍSICA E APP
app_lakes AS (
    SELECT 
        CONCAT('WATER_', CAST(water_body_id AS STRING)) as restriction_id, 
        'APP MASSA DAGUA' as restriction_name,
        'APP_ZONE' as restriction_type, 
        'WATER_BODY' as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_ibge__massas_agua_app') }}
),

-- NOVA INFRAESTRUTURA (RODOVIAS) - VETOR DE RISCO / LAVAGEM DE GRÃOS
risk_roads AS (
    SELECT 
        CONCAT('ROAD_', CAST(road_id AS STRING)) as restriction_id, 
        'EIXO RODOVIARIO - VETOR LOGISTICO' as restriction_name,
        'INFRASTRUCTURE' as restriction_type, 
        'ROAD' as restriction_subtype,
        NULL as legal_reserve_perc, 
        FALSE as is_hard_block, -- Rodovia não bloqueia, ela sinaliza risco
        3 as priority_level,
        SAFE.ST_GEOGFROMTEXT(geometry_wkt, make_valid => TRUE) as geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_ibge__rodovias_risco') }}
),

quilombolas AS (
    SELECT 
        CONCAT('INCRA_Q_', CAST(quilombo_id AS STRING)) as restriction_id, 
        quilombo_name as restriction_name,
        'QUILOMBOLA' as restriction_type, 
        certification_phase as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_incra_quilombola_lands') }}
),

uc_icmbio AS (
    SELECT 
        CONCAT('ICMBIO_', CAST(uc_id AS STRING)) as restriction_id, 
        uc_name as restriction_name,
        'CONSERVATION_UNIT' as restriction_type, 
        category as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_icmbio__unidades_conservacao') }}
),

uc_sema AS (
    SELECT 
        CONCAT('SEMA_', CAST(uc_id AS STRING)) as restriction_id, 
        uc_name as restriction_name,
        'CONSERVATION_UNIT' as restriction_type, 
        category as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_sema_mt__unidades_conservacao') }}
),

assentamentos_incra AS (
    SELECT 
        CONCAT('INCRA_A_', CAST(settlement_id AS STRING)) as restriction_id, 
        settlement_name as restriction_name,
        'SETTLEMENT' as restriction_type, 
        phase as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_incra__assentamentos') }}
),

assentamentos_intermat AS (
    SELECT 
        CONCAT('INTERMAT_', CAST(settlement_id AS STRING)) as restriction_id, 
        settlement_name as restriction_name,
        'SETTLEMENT' as restriction_type, 
        phase as restriction_subtype,
        NULL as legal_reserve_perc, 
        TRUE as is_hard_block, 
        1 as priority_level,
        geometry,
        file_hash, 
        ingested_at
    FROM {{ ref('stg_intermat__assentamentos') }}
),

unioned AS (
    SELECT * FROM biomes UNION ALL
    SELECT * FROM indigenous UNION ALL
    SELECT * FROM app_rivers UNION ALL
    SELECT * FROM app_lakes UNION ALL
    SELECT * FROM risk_roads UNION ALL
    SELECT * FROM quilombolas UNION ALL
    SELECT * FROM uc_icmbio UNION ALL
    SELECT * FROM uc_sema UNION ALL
    SELECT * FROM assentamentos_incra UNION ALL
    SELECT * FROM assentamentos_intermat
)

SELECT
    *,
    ST_BOUNDINGBOX(geometry) as bbox
FROM unioned
WHERE geometry IS NOT NULL
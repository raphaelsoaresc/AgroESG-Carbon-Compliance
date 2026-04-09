{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by=['feature_type', 'feature_subtype']
) }}

WITH roads AS (
    SELECT 
        CONCAT('ROAD_', CAST(ABS(FARM_FINGERPRINT(ST_ASBINARY(geometry))) AS STRING)) as feature_id,
        COALESCE(road_code, 'S/N') as feature_name,
        'LOGISTICS' as feature_type,
        'ROAD' as feature_subtype,
        CONCAT(
            'Acesso logístico via ', COALESCE(road_code, 'Estrada não identificada'), 
            ' (Revestimento: ', COALESCE(surface_type, 'Não Informado'), 
            ' | Jurisdição: ', COALESCE(jurisdiction, 'Não Informada'), ')'
        ) as evidence_description,
        is_operational,
        2 as priority_level,
        geometry,
        ingested_at
    FROM {{ ref('stg_ibge_bc250__rodovias') }}
),

airstrips AS (
    SELECT 
        CONCAT('AIR_', CAST(ABS(FARM_FINGERPRINT(ST_ASBINARY(geometry))) AS STRING)) as feature_id,
        COALESCE(airstrip_name, 'PISTA SEM NOME') as feature_name,
        'LOGISTICS' as feature_type,
        'AIRSTRIP' as feature_subtype,
        CONCAT(
            'Pista de Pouso: ', COALESCE(airstrip_name, 'SEM NOME'), 
            ' | Uso: ', COALESCE(usage_type, 'Não Informado'), 
            ' | Homologação: ', COALESCE(certification_status, 'Não Homologada'),
            ' | Tipo: ', COALESCE(airstrip_type, 'Não Informado')
        ) as evidence_description,
        is_operational,
        1 as priority_level,
        geometry,
        ingested_at
    FROM {{ ref('stg_ibge_bc250__pistas_pouso') }}
),

power_lines AS (
    SELECT 
        CONCAT('POWER_', CAST(ABS(FARM_FINGERPRINT(ST_ASBINARY(geometry))) AS STRING)) as feature_id,
        COALESCE(line_name, 'LINHA SEM NOME') as feature_name,
        'INFRASTRUCTURE' as feature_type,
        'POWER_LINE' as feature_subtype,
        CONCAT(
            'Linha de Energia: ', COALESCE(line_name, 'Linhão sem nome oficial'), 
            ' | Tipo: ', COALESCE(energy_type, 'Distribuição/Transmissão'),
            CASE WHEN is_national_grid THEN ' | Integrada ao SIN' ELSE '' END
        ) as evidence_description,
        is_operational,
        3 as priority_level,
        geometry,
        ingested_at
    FROM {{ ref('stg_ibge_bc250_linhas_energia') }}
),

water_bodies AS (
    SELECT 
        CONCAT('WATER_BODY_', CAST(ABS(FARM_FINGERPRINT(ST_ASBINARY(geometry))) AS STRING)) as feature_id,
        COALESCE(water_body_name, 'CORPO D\'ÁGUA SEM NOME') as feature_name,
        'WATER_RESOURCE' as feature_type,
        'WATER_BODY' as feature_subtype,
        CONCAT(
            'Massa d\'água: ', COALESCE(water_body_name, 'Corpo d\'água sem nome'), 
            ' | Tipo: ', COALESCE(water_body_type, 'Não Identificado'),
            CASE WHEN is_man_made THEN ' (Reservatório Artificial/Represa)' ELSE ' (Natural)' END
        ) as evidence_description,
        TRUE as is_operational,
        2 as priority_level,
        geometry,
        ingested_at
    FROM {{ ref('stg_ibge_bc250_massas_agua') }}
),

rivers AS (
    SELECT 
        CONCAT('RIVER_', CAST(ABS(FARM_FINGERPRINT(ST_ASBINARY(geometry))) AS STRING)) as feature_id,
        COALESCE(river_name, 'RIO SEM NOME') as feature_name,
        'WATER_RESOURCE' as feature_type,
        'RIVER' as feature_subtype,
        CONCAT(
            'Curso d\'água: ', COALESCE(river_name, 'Rio sem nome oficial'), 
            CASE WHEN is_navigable THEN ' | ROTA FLUVIAL NAVEGÁVEL' ELSE ' | Não navegável' END,
            ' | Regime: ', COALESCE(water_regime, 'Não informado')
        ) as evidence_description,
        TRUE as is_operational,
        2 as priority_level,
        geometry,
        ingested_at
    FROM {{ ref('stg_ibge_bc250_rios_linhas') }}
),

unioned AS (
    SELECT * FROM roads UNION ALL
    SELECT * FROM airstrips UNION ALL
    SELECT * FROM power_lines UNION ALL
    SELECT * FROM water_bodies UNION ALL
    SELECT * FROM rivers
)

SELECT
    *,
    ST_BOUNDINGBOX(geometry) as bbox
FROM unioned
WHERE geometry IS NOT NULL
-- O PULO DO GATO: Se houver geometrias idênticas, mantém apenas a mais recente
QUALIFY ROW_NUMBER() OVER (PARTITION BY feature_id ORDER BY ingested_at DESC) = 1
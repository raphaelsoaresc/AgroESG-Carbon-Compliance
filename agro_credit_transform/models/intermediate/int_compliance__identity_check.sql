{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='tax_id',
    tags=['compliance']
) }}

WITH slave_labor AS (
    SELECT 
        -- Garante que o ID é apenas números para o JOIN funcionar perfeitamente
        REGEXP_REPLACE(tax_id, r'\D', '') AS clean_tax_id,
        tax_id AS original_tax_id,
        employer_name,
        inclusion_date,
        workers_count,
        property_location_raw
    FROM {{ ref('stg_mte_slave_labor') }}
    WHERE tax_id IS NOT NULL
),

ibama_agg AS (
    SELECT 
        REGEXP_REPLACE(tax_id, r'\D', '') AS clean_tax_id,
        -- Pegamos o primeiro tax_id não nulo para referência
        ANY_VALUE(tax_id) AS original_tax_id,
        -- Métricas agregadas (caso tenha múltiplos embargos)
        COUNT(*) AS total_embargoes,
        LOGICAL_OR(is_active_embargo) AS has_active_embargo,
        SUM(COALESCE(reported_area_ha, 0)) AS total_ibama_area_ha,
        MAX(embargo_date) AS last_embargo_date,
        -- Guardamos as geometrias em um array para não duplicar linhas
        ARRAY_AGG(
            STRUCT(embargo_date, is_active_embargo, geometry_wkt)
        ) AS ibama_details
    FROM {{ ref('stg_ibama') }}
    WHERE tax_id IS NOT NULL
    GROUP BY 1
)

SELECT
    -- Prioriza o ID que existir (se vier de um lado ou do outro)
    COALESCE(s.clean_tax_id, i.clean_tax_id) AS tax_id,
    
    -- Nome: Prioriza o do Trabalho Escravo (geralmente mais atualizado em PF), senão NULL
    s.employer_name,

    -- Dados Trabalho Escravo
    CASE WHEN s.clean_tax_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_slave_labor_risk,
    s.inclusion_date AS slave_labor_inclusion_date,
    s.workers_count,
    s.property_location_raw,

    -- Dados IBAMA
    CASE WHEN i.clean_tax_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_ibama_risk,
    i.total_embargoes,
    i.has_active_embargo,
    i.total_ibama_area_ha,
    i.last_embargo_date,
    i.ibama_details, -- Array com geometrias e detalhes

    -- Flag de Reincidência Interagências (O "Pior dos Mundos")
    CASE 
        WHEN s.clean_tax_id IS NOT NULL AND i.clean_tax_id IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS is_recidivist_inter_agency

FROM slave_labor s
FULL OUTER JOIN ibama_agg i 
    ON s.clean_tax_id = i.clean_tax_id
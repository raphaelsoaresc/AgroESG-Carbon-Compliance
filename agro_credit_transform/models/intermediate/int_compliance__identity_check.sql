{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='tax_id',
    tags=['compliance']
) }}

WITH slave_labor AS (
    SELECT 
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
        ANY_VALUE(tax_id) AS original_tax_id,
        
        -- 🟢 AJUSTE DE SEGURANÇA: Só conta embargos que NÃO foram cancelados
        COUNT(CASE WHEN is_cancelled = FALSE THEN 1 END) AS total_embargoes,
        LOGICAL_OR(is_active_embargo AND is_cancelled = FALSE) AS has_active_embargo,
        SUM(CASE WHEN is_cancelled = FALSE THEN COALESCE(reported_area_ha, 0) ELSE 0 END) AS total_ibama_area_ha,
        
        MAX(embargo_date) AS last_embargo_date,
        
        -- 🟢 CORREÇÃO AQUI: Trocamos geometry_wkt por geometry
        ARRAY_AGG(
            STRUCT(
                embargo_id, 
                embargo_date, 
                is_active_embargo, 
                is_cancelled, 
                geometry 
            )
        ) AS ibama_details
    FROM {{ ref('stg_ibama') }}
    WHERE tax_id IS NOT NULL
    GROUP BY 1
)

SELECT
    COALESCE(s.clean_tax_id, i.clean_tax_id) AS tax_id,
    s.employer_name,

    -- Dados Trabalho Escravo (Nomes mantidos)
    CASE WHEN s.clean_tax_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_slave_labor_risk,
    s.inclusion_date AS slave_labor_inclusion_date,
    s.workers_count,
    s.property_location_raw,

    -- Dados IBAMA (Nomes mantidos, lógica protegida)
    CASE WHEN i.clean_tax_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_ibama_risk,
    i.total_embargoes,
    i.has_active_embargo,
    i.total_ibama_area_ha,
    i.last_embargo_date,
    i.ibama_details, 

    -- Flag de Reincidência
    CASE 
        WHEN s.clean_tax_id IS NOT NULL AND i.clean_tax_id IS NOT NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS is_recidivist_inter_agency

FROM slave_labor s
FULL OUTER JOIN ibama_agg i 
    ON s.clean_tax_id = i.clean_tax_id
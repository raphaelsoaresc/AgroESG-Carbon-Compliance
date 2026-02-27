{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='tax_id',
    tags=['compliance']
) }}

WITH slave_labor AS (
    SELECT 
        tax_id,
        employer_name,
        inclusion_date AS slave_labor_inclusion_date,
        workers_count,
        property_location_raw
    FROM {{ ref('stg_mte_slave_labor') }}
),

ibama AS (
    SELECT 
        tax_id,
        embargo_date AS ibama_embargo_date,
        reported_area_ha AS ibama_area_ha,
        is_active_embargo,
        geometry_wkt AS ibama_geometry_wkt
    FROM {{ ref('stg_ibama') }}
)

SELECT
    s.tax_id,
    s.employer_name,
    s.slave_labor_inclusion_date,
    s.workers_count,
    s.property_location_raw AS slave_labor_location_string,
    i.ibama_embargo_date,
    i.ibama_area_ha,
    i.is_active_embargo,
    i.ibama_geometry_wkt,
    -- Flag de Identidade: O CPF/CNPJ manchado em ambas as esferas
    TRUE AS is_recidivist_inter_agency
FROM slave_labor s
INNER JOIN ibama i 
    ON s.tax_id = i.tax_id
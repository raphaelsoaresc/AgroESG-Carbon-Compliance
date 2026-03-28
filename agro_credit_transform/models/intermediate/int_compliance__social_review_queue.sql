{{ config(materialized='table', schema='agro_esg_intermediate') }}

SELECT
    slave_labor_tax_id,
    employer_name,
    sigef_property_id,
    sigef_property_name,
    mte_uf,
    territorial_match_confidence,
    -- Campos para sua intervenção manual no futuro (via BI ou App)
    CAST(NULL AS STRING) as human_decision, 
    CAST(NULL AS STRING) as technologist_notes,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('int_compliance__slave_labor_sigef_bridge') }}
WHERE territorial_match_confidence IN ('HIGH_PROBABILITY', 'MEDIUM_FUZZY_MATCH')
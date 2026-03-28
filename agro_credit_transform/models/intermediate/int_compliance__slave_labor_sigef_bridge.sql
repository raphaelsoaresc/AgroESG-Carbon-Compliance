{{ config(materialized='table', schema='agro_esg_intermediate') }}

WITH slave_labor AS (
    SELECT * FROM {{ ref('int_mte__slave_labor_normalization') }}
),

sigef AS (
    SELECT 
        property_id, property_name, state_abbreviation as sigef_uf,
        geometry_wkt,
        UPPER(TRIM(REGEXP_REPLACE(NORMALIZE(property_name, NFD), r"\pM", ""))) as sigef_name_normalized
    FROM {{ ref('stg_sigef') }}
),

-- 1. Realiza o Match inicial
raw_matches AS (
    SELECT
        s.tax_id as slave_labor_tax_id,
        s.employer_name,
        s.farm_name_normalized as slave_labor_farm_name,
        sig.property_id as sigef_property_id,
        sig.property_name as sigef_property_name,
        sig.geometry_wkt as sigef_geometry_wkt,
        s.mte_uf,
        
        CASE 
            WHEN s.farm_name_normalized = sig.sigef_name_normalized 
                 AND s.mte_uf = sig.sigef_uf THEN 'HIGH_PROBABILITY_NAME_MATCH'
            WHEN (STRPOS(sig.sigef_name_normalized, s.farm_core_name) > 0)
                 AND s.mte_uf = sig.sigef_uf THEN 'MEDIUM_FUZZY_MATCH'
            ELSE 'LOW_CONFIDENCE'
        END as initial_confidence
    FROM slave_labor s
    INNER JOIN sigef sig 
        ON s.mte_uf = sig.sigef_uf 
        AND LENGTH(TRIM(s.farm_core_name)) > 3
        AND (STRPOS(sig.sigef_name_normalized, s.farm_core_name) > 0 
             OR STRPOS(s.farm_core_name, sig.sigef_name_normalized) > 0)
),

-- 2. 🛡️ PROTEÇÃO CONTRA EXPLOSÃO (O Pulo do Gato)
match_counts AS (
    SELECT 
        *,
        COUNT(*) OVER(PARTITION BY slave_labor_tax_id, slave_labor_farm_name) as total_properties_matched
    FROM raw_matches
)

-- 3. Veredito Final da Ponte
SELECT
    * EXCEPT(initial_confidence, total_properties_matched),
    CASE 
        -- Se o nome casou com mais de 15 fazendas, é um nome genérico (ex: Santa Maria). 
        -- Rebaixamos para LOW para não bloquear ninguém por engano.
        WHEN total_properties_matched > 15 THEN 'LOW_CONFIDENCE_GENERIC_NAME'
        ELSE initial_confidence
    END as territorial_match_confidence,
    total_properties_matched
FROM match_counts
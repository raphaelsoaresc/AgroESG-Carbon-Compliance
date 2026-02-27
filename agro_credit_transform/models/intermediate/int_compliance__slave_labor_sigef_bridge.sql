{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='sigef_property_id',
    tags=['compliance']
) }}

WITH slave_labor AS (
    SELECT 
        *,
        -- Garante que a UF extraída não tenha espaços ou pontos
        TRIM(REGEXP_REPLACE(extracted_uf, r'[\.\,]', '')) as clean_extracted_uf,
        -- Limpeza extra de abreviações no nome da fazenda da Lista Suja
        REGEXP_REPLACE(farm_name_match_key, r'\b(STA|STO|SAO|SANTA|SANTO|NOSSA SRA|N SRA|S\.)\b', '') as farm_key_no_abbr
    FROM {{ ref('int_mte__slave_labor_normalization') }}
    WHERE is_rural_target = TRUE
),

state_codes AS (
    SELECT * FROM {{ ref('br_state_codes') }}
),

sigef AS (
    SELECT 
        s.property_id AS sigef_property_id,
        s.property_name AS sigef_property_name,
        sc.state_sigla AS sigef_state,
        s.geometry_wkt,
        -- Normalização agressiva do nome no SIGEF
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    NORMALIZE(UPPER(s.property_name), NFD), 
                r"\pM", ""), 
            r'\b(FAZENDA|SITIO|ESTANCIA|GLEBA|CHACARA|PROPRIEDADE|RURAL|AGROPECUARIA)\b', ''),
        r'\b(STA|STO|SAO|SANTA|SANTO|NOSSA SRA|N SRA|S\.)\b', '') AS sigef_name_match_key
    FROM {{ ref('stg_sigef') }} s
    LEFT JOIN state_codes sc 
        ON CAST(s.state_id AS INT64) = CAST(sc.state_id AS INT64)
)

SELECT
    s.tax_id,
    s.employer_name,
    s.farm_name_raw AS slave_labor_farm_name,
    sig.sigef_property_name,
    sig.sigef_property_id,
    s.extracted_city,
    s.clean_extracted_uf as extracted_uf,
    sig.geometry_wkt AS sigef_geometry_wkt,
    -- Lógica de Confiança do Match Territorial
    CASE 
        WHEN TRIM(s.farm_key_no_abbr) = TRIM(sig.sigef_name_match_key) THEN 'HIGH'
        WHEN (STRPOS(sig.sigef_name_match_key, s.farm_key_no_abbr) > 0 OR STRPOS(s.farm_key_no_abbr, sig.sigef_name_match_key) > 0) THEN 'MEDIUM'
        ELSE 'LOW'
    END AS territorial_match_confidence
FROM slave_labor s
INNER JOIN sigef sig
    ON s.clean_extracted_uf = sig.sigef_state 
    AND (
        -- Match Exato
        TRIM(s.farm_key_no_abbr) = TRIM(sig.sigef_name_match_key)
        OR 
        -- Match Parcial (Contém): Aumenta a chance de encontrar resultados
        (LENGTH(TRIM(s.farm_key_no_abbr)) > 4 AND STRPOS(sig.sigef_name_match_key, s.farm_key_no_abbr) > 0)
    )
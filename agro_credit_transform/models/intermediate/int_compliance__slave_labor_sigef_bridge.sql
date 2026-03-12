{{ config(
    materialized='table',
    schema='agro_esg_intermediate',
    cluster_by='sigef_property_id',
    tags=['compliance']
) }}

WITH slave_labor AS (
    SELECT 
        *,
        TRIM(REGEXP_REPLACE(extracted_uf, r'[\.\,]', '')) as clean_extracted_uf,
        REGEXP_REPLACE(farm_name_match_key, r'\b(STA|STO|SAO|SANTA|SANTO|NOSSA SRA|N SRA|S\.)\b', '') as farm_key_no_abbr
    FROM {{ ref('int_mte__slave_labor_normalization') }}
    WHERE is_rural_target = TRUE
      -- 🛡️ DESLIGANDO A ESPINGARDA PARA NOMES GENÉRICOS REAIS
      -- Proibimos o Fuzzy Match para esse infrator porque o nome da fazenda dele ("Sol Nascente")
      -- gera 85 falsos positivos. Ele será pego EXCLUSIVAMENTE pelo match exato de CPF.
      AND employer_name != 'ANTONIO BORGES BELFORT'
),

state_codes AS (
    SELECT * FROM {{ ref('br_state_codes') }}
),

sigef_base AS (
    SELECT 
        s.property_id AS sigef_property_id,
        s.property_name AS sigef_property_name,
        sc.state_sigla AS sigef_state,
        s.geometry_wkt,
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
),

-- 🛡️ NOVO: ESCUDO ANTI-NOMES GENÉRICOS
-- Conta quantas vezes cada nome limpo aparece no estado
sigef_name_counts AS (
    SELECT 
        sigef_state, 
        TRIM(sigef_name_match_key) as match_key, 
        COUNT(*) as name_frequency
    FROM sigef_base
    WHERE TRIM(sigef_name_match_key) != ''
    GROUP BY 1, 2
),

sigef AS (
    SELECT 
        b.*,
        COALESCE(c.name_frequency, 0) as name_frequency
    FROM sigef_base b
    LEFT JOIN sigef_name_counts c 
        ON b.sigef_state = c.sigef_state 
        AND TRIM(b.sigef_name_match_key) = c.match_key
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
    CASE 
        WHEN TRIM(s.farm_key_no_abbr) = TRIM(sig.sigef_name_match_key) THEN 'HIGH'
        WHEN (STRPOS(sig.sigef_name_match_key, s.farm_key_no_abbr) > 0 OR STRPOS(s.farm_key_no_abbr, sig.sigef_name_match_key) > 0) THEN 'MEDIUM'
        ELSE 'LOW'
    END AS territorial_match_confidence
FROM slave_labor s
INNER JOIN sigef sig
    ON s.clean_extracted_uf = sig.sigef_state 
    AND (
        TRIM(s.farm_key_no_abbr) = TRIM(sig.sigef_name_match_key)
        OR 
        (LENGTH(TRIM(s.farm_key_no_abbr)) > 10 AND STRPOS(sig.sigef_name_match_key, s.farm_key_no_abbr) > 0)
    )
-- A MÁGICA ACONTECE AQUI:
-- Se o nome da fazenda se repete mais de 5 vezes no mesmo estado, é genérico demais. 
-- Nós bloqueamos o match para proteger produtores inocentes.
WHERE sig.name_frequency <= 5
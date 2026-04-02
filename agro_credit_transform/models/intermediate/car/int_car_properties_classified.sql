{{ config(
    materialized='view',
    schema='agro_esg_intermediate'
) }}

WITH properties_meta AS (
    SELECT
        p.property_id,
        p.area_ha,
        p.fiscal_modules,
        p.city as city_name, 
        p.state as uf_origem,
        c.city_id as city_ibge_id
    FROM {{ ref('stg_car_properties') }} p
    -- 1. Buscamos o state_id através da sigla (UF)
    LEFT JOIN {{ ref('br_state_codes') }} sc
        ON UPPER(TRIM(p.state)) = UPPER(TRIM(sc.state_sigla))
    
    -- 2. Buscamos a cidade pelo nome E validamos se os 2 primeiros dígitos do city_id batem com o state_id
    LEFT JOIN {{ ref('br_ibge_cities') }} c 
        ON UPPER(TRIM(p.city)) = UPPER(TRIM(c.city_name))
        AND LEFT(CAST(c.city_id AS STRING), 2) = CAST(sc.state_id AS STRING)
),

sobreposicao_meta AS (
    SELECT
        property_id,
        ANY_VALUE(property_area_ha) as property_area_ha,
        ANY_VALUE(fiscal_modules) as fiscal_modules,
        ANY_VALUE(city_ibge_id) as city_ibge_id,
        ANY_VALUE(uf) as uf
    FROM {{ ref('stg_car_overlaps') }}
    GROUP BY 1
),

incra_indices AS (
    SELECT
        city_ibge_id,
        fiscal_module_ha,
        fmp_ha
    FROM {{ ref('stg_incra_municipal_indices') }}
),

consolidated_base AS (
    SELECT
        p.property_id,
        COALESCE(p.area_ha, s.property_area_ha, 0) as final_area_ha,
        COALESCE(p.fiscal_modules, s.fiscal_modules, 0) as final_fiscal_modules,
        COALESCE(s.city_ibge_id, p.city_ibge_id) as final_city_ibge_id,
        COALESCE(p.uf_origem, s.uf) as final_uf
    FROM properties_meta p
    LEFT JOIN sobreposicao_meta s ON p.property_id = s.property_id
),

joined_with_incra AS (
    SELECT
        b.*,
        i.fmp_ha,
        i.fiscal_module_ha as incra_fiscal_module_ha
    FROM consolidated_base b
    LEFT JOIN incra_indices i ON b.final_city_ibge_id = i.city_ibge_id
)

SELECT
    *,
    CASE 
        -- TRAVA DE SEGURANÇA: Se a área for maior que 1500ha, é GRANDE independente do erro no campo de módulos
        WHEN final_area_ha > 1500 THEN 'GRANDE PROPRIEDADE'
        
        WHEN fmp_ha IS NOT NULL AND final_area_ha < fmp_ha THEN 'MINIFÚNDIO'
        WHEN fmp_ha IS NOT NULL AND final_area_ha >= fmp_ha AND final_fiscal_modules <= 4 THEN 'PEQUENA PROPRIEDADE'
        WHEN fmp_ha IS NULL AND final_fiscal_modules <= 4 THEN 'PEQUENA PROPRIEDADE'
        WHEN final_fiscal_modules > 4 AND final_fiscal_modules <= 15 THEN 'MÉDIA PROPRIEDADE'
        WHEN final_fiscal_modules > 15 THEN 'GRANDE PROPRIEDADE'
        ELSE 'NÃO CLASSIFICADO'
    END as producer_size_category,

    CASE 
        -- Se cair na trava de segurança (>1500ha), não pode ser considerado Small Holder
        WHEN final_area_ha > 1500 THEN FALSE
        
        WHEN (fmp_ha IS NOT NULL AND final_area_ha < fmp_ha) 
             OR final_fiscal_modules <= 4 THEN TRUE 
        ELSE FALSE 
    END as is_small_holder

FROM joined_with_incra
{{ config(
    materialized='view',
    schema='agro_esg_intermediate',
    tags=['car']
) }}

WITH properties_meta AS (
    -- Agora usamos a stg_car_owners que limpamos e padronizamos
    SELECT
        p.property_id,
        p.area_do_imovel as area_ha, -- Já vem com a trava de sanidade da staging
        p.modulos_fiscais as fiscal_modules,
        p.municipio as city_name, 
        p.uf as uf_origem,
        p.codigo_ibge as city_ibge_id,
        p.registration_status,
        p.tipo_imovel_rural as property_type
    FROM {{ ref('stg_car_owners') }} p
),

sobreposicao_meta AS (
    -- Usamos a stg_car_overlaps que também já tem a trava de sanidade
    SELECT
        property_id,
        ANY_VALUE(property_area_ha) as property_area_ha,
        ANY_VALUE(fiscal_modules) as fiscal_modules,
        ANY_VALUE(codigo_ibge) as city_ibge_id, -- Ajustado nome da coluna
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
        -- Priorizamos a área da stg_car_owners, se nula, tentamos a da sobreposição
        COALESCE(p.area_ha, s.property_area_ha, 0) as final_area_ha,
        COALESCE(p.fiscal_modules, s.fiscal_modules, 0) as final_fiscal_modules,
        COALESCE(p.city_ibge_id, s.city_ibge_id) as final_city_ibge_id,
        COALESCE(p.uf_origem, s.uf) as final_uf,
        p.registration_status,
        p.property_type
    FROM properties_meta p
    FULL OUTER JOIN sobreposicao_meta s ON p.property_id = s.property_id
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
    property_id,
    final_area_ha,
    final_fiscal_modules,
    final_city_ibge_id,
    final_uf,
    registration_status,
    property_type,
    fmp_ha,
    incra_fiscal_module_ha,
    
    -- Lógica de Classificação de Tamanho
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

    -- Lógica de Small Holder (Agricultura Familiar)
    CASE 
        WHEN final_area_ha > 1500 THEN FALSE
        WHEN (fmp_ha IS NOT NULL AND final_area_ha < fmp_ha) 
             OR final_fiscal_modules <= 4 THEN TRUE 
        ELSE FALSE 
    END as is_small_holder

FROM joined_with_incra
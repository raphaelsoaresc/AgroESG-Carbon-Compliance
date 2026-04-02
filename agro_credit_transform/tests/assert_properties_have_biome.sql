-- tests/assert_imoveis_possuem_bioma.sql
SELECT
    property_id,
    biome_name
FROM {{ ref('fct_compliance_risk') }} -- substitua pelo nome deste modelo
WHERE (biome_name IS NULL OR biome_name = 'N/A' OR TRIM(biome_name) = '')
  -- Ignora imóveis que já sabemos que estão sem geometria (não há o que fazer espacialmente)
  AND is_missing_geometry = FALSE 
  AND final_eligibility_status != 'NOT ELIGIBLE - INVALID GEOMETRY'
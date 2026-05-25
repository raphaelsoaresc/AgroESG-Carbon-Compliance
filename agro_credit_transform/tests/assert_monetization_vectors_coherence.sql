SELECT
    property_id,
    river_magnitude,
    monetization_vectors
FROM {{ ref('fct_property_investment_intelligence') }}
WHERE river_magnitude >= 4 
  AND monetization_vectors NOT LIKE '%Irrigação%'
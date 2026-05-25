SELECT
    property_id,
    investment_score,
    investment_rating
FROM {{ ref('fct_property_investment_intelligence') }}
WHERE (investment_score >= 80 AND investment_rating != 'AAA - Premium Asset')
   OR (investment_score < 20 AND investment_rating = 'AAA - Premium Asset')
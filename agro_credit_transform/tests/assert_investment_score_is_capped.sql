SELECT
    property_id,
    raw_asset_score,
    investment_score
FROM {{ ref('fct_property_investment_intelligence') }}
WHERE investment_score > (raw_asset_score + 0.01) -- margem para erro de arredondamento
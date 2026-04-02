WITH validation AS (
    SELECT 
        f.property_id,
        f.biome_name,
        m.legal_reserve_perc
    FROM {{ ref('fct_compliance_risk') }} f
    LEFT JOIN {{ ref('int_car_compliance_metrics') }} m ON f.property_id = m.property_id
)
SELECT *
FROM validation
WHERE 
    (biome_name = 'AMAZÔNIA' AND legal_reserve_perc != 0.80)
    OR
    (biome_name = 'CERRADO' AND legal_reserve_perc != 0.35)
    OR
    (biome_name = 'PANTANAL' AND legal_reserve_perc != 0.20)
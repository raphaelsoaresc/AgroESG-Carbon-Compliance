-- Falha se a % de reserva legal não bater com a regra do bioma

SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    (biome_name = 'AMAZÔNIA' AND legal_reserve_req != 0.80)
    OR
    (biome_name = 'CERRADO' AND legal_reserve_req != 0.35)
    OR
    (biome_name NOT IN ('AMAZÔNIA', 'CERRADO', 'DESCONHECIDO') AND legal_reserve_req != 0.20)
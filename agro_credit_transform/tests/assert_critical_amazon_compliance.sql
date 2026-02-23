-- Falha se existir qualquer propriedade marcada como ELIGIBLE
-- mas que tenha embargo na AMAZÔNIA após 22/07/2008.
-- EXCEÇÃO: Ignoramos ruídos de GPS (overlap < 0.001 ha), pois a regra de negócio permite.

SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    biome_name LIKE 'AMAZ%NIA'
    AND final_embargo_date >= '2008-07-22'
    AND eligibility_status LIKE 'ELIGIBLE%'
    
    AND embargo_overlap_ha >= 0.001
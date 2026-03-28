SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    -- Erro 1: É elegível mas tem embargo ATIVO (Pós-2008 ou sem data)
    (
        final_eligibility_status = 'ELIGIBLE' 
        AND embargo_area_ha > 0.1 
        AND (embargo_date >= '2008-07-22' OR embargo_date = '1900-01-01')
    )
    
    OR
    
    -- Erro 2: O status diz que é violação de EMBARGO, mas a data está nula (não deveria ocorrer com o COALESCE)
    (
        final_eligibility_status LIKE '%EMBARGO%' 
        AND (embargo_date IS NULL)
    )
    
    OR

    -- Erro 3: É elegível mas tem desmatamento MapBiomas ATIVO (Pós-2008)
    (
        final_eligibility_status = 'ELIGIBLE' 
        AND mapbiomas_deforested_ha > 0.1 
        AND (mapbiomas_detection_date >= '2008-07-22' OR mapbiomas_detection_date IS NULL)
    )
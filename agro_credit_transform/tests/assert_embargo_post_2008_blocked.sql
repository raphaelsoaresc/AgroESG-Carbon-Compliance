-- Garante que o Marco Temporal do Código Florestal (Julho de 2008) está a ser respeitado rigorosamente.
-- Nenhuma propriedade com embargo do IBAMA (área > 0.1 ha) pós-2008 pode receber o status de Elegível.

SELECT 
    property_id,
    final_eligibility_status,
    embargo_date,
    embargo_area_ha
FROM {{ ref('fct_compliance_risk') }}
WHERE embargo_date >= '2008-07-22'
    AND embargo_area_ha > 0.1
    -- Se o status final NÃO for 'NOT ELIGIBLE...', o teste apanha o erro
    AND final_eligibility_status NOT LIKE 'NOT ELIGIBLE%'
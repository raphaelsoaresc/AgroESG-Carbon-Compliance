-- tests/assert_car_status_is_active.sql
-- Objetivo: Garantir que CARs irregulares na fonte consolidada não passem como 'ELIGIBLE' puro.
-- Mudança Forense: O teste agora consome a 'int_compliance__property_analysis' em vez da staging bruta.
-- Isso evita que registros duplicados na origem (ex: um ATIVO e um CANCELADO para o mesmo ID) confundam o teste.

WITH analysis_source AS (
    -- Pegamos o status que o motor do Caipora consolidou (já limpo e deduplicado)
    SELECT 
        UPPER(TRIM(property_id)) as property_id,
        registration_status as consolidated_source_status
    FROM {{ ref('int_compliance__property_analysis') }}
),

mart AS (
    -- Pegamos o veredito final que foi para o BI/Relatório
    SELECT 
        UPPER(TRIM(property_id)) as property_id, 
        final_eligibility_status 
    FROM {{ ref('fct_compliance_risk') }}
)

SELECT 
    m.property_id, 
    m.final_eligibility_status, 
    s.consolidated_source_status
FROM mart m
JOIN analysis_source s ON m.property_id = s.property_id
WHERE 
    (
        -- FATO 1: Se o status consolidado das fontes oficiais é Cancelado ou Suspenso
        s.consolidated_source_status IN ('CANCELADO', 'SUSPENSO')
        -- O Caipora NÃO PODE rotular como 'ELIGIBLE' (nem para produtores especiais), 
        -- pois o documento é juridicamente inexistente ou inválido.
        AND (
            m.final_eligibility_status = 'ELIGIBLE' 
            OR m.final_eligibility_status LIKE 'ELIGIBLE - % PRODUCER'
        )
    )
    OR 
    (
        -- FATO 2: Se o status consolidado é Pendente (em análise pelo órgão estadual)
        s.consolidated_source_status = 'PENDENTE'
        -- O Caipora NÃO PODE rotular como 'ELIGIBLE' puro. 
        -- Ele deve estar em 'MANUAL_REVIEW' ou bloqueado por outro crime.
        AND m.final_eligibility_status = 'ELIGIBLE'
    )
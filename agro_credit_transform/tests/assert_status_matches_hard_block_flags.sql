-- tests/assert_status_matches_hard_block_flags.sql
-- Objetivo: Garantir que bloqueios técnicos resultem em 'NOT ELIGIBLE', 
-- EXCETO quando há uma identidade de produtor especial validada.

SELECT 
    property_id,
    is_technically_blocked,
    final_eligibility_status
FROM {{ ref('fct_compliance_risk') }}
WHERE 
    is_technically_blocked = TRUE 
    -- Falha se for 'ELIGIBLE' puro (sem justificativa)
    AND (
        final_eligibility_status = 'ELIGIBLE'
        -- E garante que não é um dos status de exceção (Produtores Especiais)
        AND final_eligibility_status NOT IN (
            'ELIGIBLE - INDIGENOUS PRODUCER', 
            'ELIGIBLE - CONSERVATION UNIT PRODUCER', 
            'ELIGIBLE - QUILOMBOLA PRODUCER',
            'ELIGIBLE - SETTLEMENT PRODUCER',
            'ELIGIBLE - TRADITIONAL PRODUCER',
            'ELIGIBLE - MITIGATED ADJACENCY RISK (PHYSICAL BARRIER)'
        )
        -- E não está em revisão manual (que é um estado aceitável para bloqueio técnico)
        AND final_eligibility_status NOT LIKE 'MANUAL_REVIEW%'
    )
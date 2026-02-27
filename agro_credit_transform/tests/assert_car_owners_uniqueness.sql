-- Teste de anomalia de Fan-Out (Produto Cartesiano).
-- Garante que o cruzamento de dados (como múltiplos donos para o mesmo CAR) 
-- não duplicou a propriedade na tabela fato, o que inflaria a área total em hectares.

SELECT
    property_id,
    COUNT(*) as total_ocorrencias
FROM {{ ref('fct_compliance_risk') }}
GROUP BY property_id
HAVING COUNT(*) > 1
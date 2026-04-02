-- Teste novo: assert_identity_financial_is_zero.sql
SELECT *
FROM {{ ref('fct_compliance_risk') }}
WHERE (is_settlement_identity OR is_quilombo_identity)
  AND (liability_protected_areas_brl > 0 OR protected_area_overlap_ha > 0)
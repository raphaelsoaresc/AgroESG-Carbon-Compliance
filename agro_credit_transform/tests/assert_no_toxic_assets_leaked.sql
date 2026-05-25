SELECT
    property_id,
    compliance_status
FROM {{ ref('fct_property_investment_intelligence') }}
WHERE compliance_status IN (
    'NOT ELIGIBLE - SOCIAL RISK (SLAVE LABOR)',
    'NOT ELIGIBLE - INDIGENOUS LAND',
    'NOT ELIGIBLE - CONSERVATION UNIT',
    'NOT ELIGIBLE - QUILOMBOLA (INVASION)',
    'NOT ELIGIBLE - SETTLEMENT (INVASION)',
    'NOT ELIGIBLE - TRADITIONAL TERRITORY (INVASION)'
)
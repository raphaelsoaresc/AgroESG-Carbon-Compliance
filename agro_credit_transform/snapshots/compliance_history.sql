{% snapshot compliance_history %}

{{
    config(
      target_database=target.database,
      target_schema='snapshots',
      unique_key='property_id',
      strategy='check',
      check_cols=[
          'final_eligibility_status',
          'car_status',
          'geospatial_confidence_level',
          'embargo_area_ha',
          'mapbiomas_deforested_ha',
          'protected_area_overlap_ha',
          'slave_labor_overlap_ha',
          'estimated_financial_liability_brl',
          'technical_evidence'
      ],
      invalidate_hard_deletes=True
    )
}}

-- Selecionamos todas as colunas para garantir rastreabilidade total de mudanças
-- O dbt irá comparar apenas as colunas do check_cols para decidir se gera uma nova versão
select 
    * 
from {{ ref('fct_compliance_risk') }}

{% endsnapshot %}
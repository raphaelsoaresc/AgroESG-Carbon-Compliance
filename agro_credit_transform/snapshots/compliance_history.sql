{% snapshot compliance_history %}

{{
    config(
      target_database=target.database,
      target_schema='snapshots',
      unique_key='property_id',
      strategy='check',
      check_cols=['final_eligibility_status', 'technical_evidence']
    )
}}

-- Selecionamos apenas o necessário para evitar custo de storage (sem geometrias)
select 
    property_id, 
    final_eligibility_status, 
    technical_evidence 
from {{ ref('fct_compliance_risk') }}

{% endsnapshot %}

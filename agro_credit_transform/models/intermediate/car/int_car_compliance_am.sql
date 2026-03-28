-- depends_on: {{ ref('stg_car_environmental_themes') }}
-- depends_on: {{ ref('int_car_geometries') }}
-- depends_on: {{ ref('int_brazil_reference_geometries') }}

{{ config(materialized='incremental', unique_key='property_id', cluster_by='property_id') }}
{{ get_compliance_logic('AM') }}
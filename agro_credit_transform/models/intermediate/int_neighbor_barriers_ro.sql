{{ config(materialized='table', cluster_by=['property_id', 'neighbor_id']) }}
{{ get_neighbor_barriers_logic('RO') }}
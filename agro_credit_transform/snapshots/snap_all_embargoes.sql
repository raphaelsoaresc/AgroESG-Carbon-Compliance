{% snapshot snap_all_embargoes %}

{{ config(target_schema='agro_esg_snapshots', unique_key='snapshot_id', strategy='check', check_cols=['is_active_embargo', 'is_cancelled', 'reported_area_ha', 'process_number'], invalidate_hard_deletes=True) }}

SELECT 
    CONCAT(source, '_', embargo_id) AS snapshot_id,
    embargo_id,
    source,
    tad_number,
    process_number,
    tax_id,
    offender_name,
    property_name_raw,
    embargo_date,
    reported_area_ha,
    is_active_embargo,
    is_cancelled,
    geometry,
    longitude,
    latitude,
    file_hash,
    ingested_at
FROM {{ ref('int_all_embargoes') }}

{% endsnapshot %}
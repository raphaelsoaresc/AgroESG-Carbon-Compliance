SELECT 
    cobacia,
    river_order,
    app_width_m,
    ST_GEOGFROMTEXT(wkt_geom_app, make_valid => TRUE) as geometry,
    file_hash,
    ingested_at
FROM {{ source('raw_data', 'ana_app_zones') }}
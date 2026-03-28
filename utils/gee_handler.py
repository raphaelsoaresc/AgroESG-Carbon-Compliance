import ee
import logging
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

def initialize_gee(gcp_conn_id='google_cloud_default'):
    try:
        ee.data.getAssetRoots()
    except Exception:
        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = hook.get_credentials()
        project_id = hook.project_id
        ee.Initialize(credentials=credentials, project=project_id)
        logging.info(f"🛰️ GEE conectado: {project_id}")

def get_mt_fc(asset_id):
    """Limpa o Asset para o mínimo absoluto de memória."""
    return ee.FeatureCollection(asset_id).select([])

def export_topography_stats(asset_id, bucket_name, output_prefix):
    """SRTM - Topografia (Separado)"""
    initialize_gee()
    fc = get_mt_fc(asset_id)
    topo_image = ee.Image("USGS/SRTMGL1_003").select(['elevation'])
    
    stats = topo_image.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.mean(),
        scale=30,
        tileScale=16
    )

    # Correção do filtro (List<String>)
    stats_clean = stats.filter(ee.Filter.notNull(['elevation']))

    task = ee.batch.Export.table.toCloudStorage(
        collection=stats_clean,
        description='export_topo_srtm',
        bucket=bucket_name,
        fileNamePrefix=f"{output_prefix}_topography",
        fileFormat='CSV'
    )
    task.start()
    return task.id

def export_elevation_stats(asset_id, bucket_name, output_prefix):
    """JAXA - Elevação (Separado)"""
    initialize_gee()
    fc = get_mt_fc(asset_id)
    
    # JAXA V3.2 (Usando mean() para garantir que vire uma Image)
    jaxa_image = ee.ImageCollection("JAXA/ALOS/AW3D30/V3_2").select('DSM').mean()
    
    stats = jaxa_image.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.mean(),
        scale=30,
        tileScale=16
    )

    # Correção do filtro (List<String>)
    stats_clean = stats.filter(ee.Filter.notNull(['DSM']))

    task = ee.batch.Export.table.toCloudStorage(
        collection=stats_clean,
        description='export_elevation_jaxa',
        bucket=bucket_name,
        fileNamePrefix=f"{output_prefix}_elevation",
        fileFormat='CSV'
    )
    task.start()
    return task.id

def export_mapbiomas_stats(asset_id, year, bucket_name, output_prefix):
    """MapBiomas (Separado)"""
    initialize_gee()
    fc = get_mt_fc(asset_id)
    
    asset_path = (
        "projects/mapbiomas-public/assets/brazil/lulc_10m/collection2/mapbiomas_10m_collection2_integration_v1" 
        if year >= 2016 else 
        "projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1"
    )
    
    band_name = f'classification_{year}'
    lulc_img = ee.Image(asset_path).select(band_name)
    
    stats = lulc_img.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.mode(),
        scale=30,
        tileScale=16
    )

    # Correção do filtro (List<String>)
    stats_clean = stats.filter(ee.Filter.notNull([band_name]))

    task = ee.batch.Export.table.toCloudStorage(
        collection=stats_clean,
        description=f'export_mapbiomas_{year}',
        bucket=bucket_name,
        fileNamePrefix=f"{output_prefix}_mapbiomas_{year}",
        fileFormat='CSV'
    )
    task.start()
    return task.id
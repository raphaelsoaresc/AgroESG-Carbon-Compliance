import ee
import logging
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

def initialize_gee(gcp_conn_id='google_cloud_default'):
    try:
        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = hook.get_credentials()
        project_id = hook.project_id
        ee.Initialize(credentials=credentials, project=project_id)
        logging.info(f"🛰️ GEE autenticado com sucesso no projeto: {project_id}")
        return True
    except Exception as e:
        logging.error(f"❌ Erro ao conectar com o GEE: {str(e)}")
        raise

def get_topography_stats(features_list):
    features = [ee.Feature(ee.Geometry(f['geometry']), {'property_id': f['property_id']}) for f in features_list]
    fc = ee.FeatureCollection(features)

    srtm = ee.Image("USGS/SRTMGL1_003")
    slope = ee.Terrain.slope(srtm)
    topo_image = srtm.addBands(slope).select(['elevation', 'slope'], ['elevation', 'slope_degrees'])

    stats = topo_image.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(),
            sharedInputs=True
        ),
        scale=30,      # PRECISÃO MÁXIMA MANTIDA (30 metros)
        tileScale=4    # SOLUÇÃO: Quebra em partições menores para não travar a memória
    )
    return stats.getInfo()['features']


def mask_s2_clouds(image):
    scl = image.select('SCL')
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11))
    return image.updateMask(mask)


def get_ndvi_stats(features_list, start_date, end_date):
    features = [ee.Feature(ee.Geometry(f['geometry']), {'property_id': f['property_id']}) for f in features_list]
    fc = ee.FeatureCollection(features)

    s2_col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(fc)
                .filterDate(start_date, end_date)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
                .map(mask_s2_clouds))

    if s2_col.size().getInfo() == 0:
        logging.warning("⚠️ Nenhuma imagem Sentinel-2 limpa.")
        return []

    def add_ndvi(img):
        ndvi = img.normalizedDifference(['B8', 'B4']).rename('ndvi')
        return img.addBands(ndvi)

    median_ndvi = s2_col.map(add_ndvi).select('ndvi').median()

    stats = median_ndvi.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.mean().combine(
            reducer2=ee.Reducer.minMax(),
            sharedInputs=True
        ),
        scale=10,      # PRECISÃO MÁXIMA MANTIDA (10 metros)
        tileScale=4    # SOLUÇÃO: Particionamento de memória
    )
    return stats.getInfo()['features']
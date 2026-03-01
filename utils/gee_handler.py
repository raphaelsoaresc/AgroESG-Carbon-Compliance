import ee
import logging
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

def initialize_gee(gcp_conn_id='google_cloud_default'):
    """
    Faz o Handshake com o Google Earth Engine utilizando as 
    credenciais da conexão padrão do Airflow.
    """
    try:
        # 1. Buscamos as credenciais que você já configurou no Airflow
        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = hook.get_credentials()
        project_id = hook.project_id

        # 2. Inicializamos a biblioteca oficial do Earth Engine
        # O parâmetro 'project' é crucial para o plano Community
        ee.Initialize(credentials=credentials, project=project_id)
        
        logging.info(f"🛰️ GEE autenticado com sucesso no projeto: {project_id}")
        return True

    except Exception as e:
        logging.error(f"❌ Erro ao conectar com o Google Earth Engine: {str(e)}")
        raise

def get_topography_stats(features_list):
    """
    Calcula estatísticas de altitude e declividade para uma lista de geometrias.
    features_list: Lista de dicionários contendo 'property_id' e 'geometry' (GeoJSON)
    """
    # 1. Converter a lista de dicionários em uma FeatureCollection do GEE
    features = []
    for f in features_list:
        # Criamos uma 'Feature' para cada fazenda, guardando o ID dela
        geom = ee.Geometry(f['geometry'])
        features.append(ee.Feature(geom, {'property_id': f['property_id']}))
    
    fc = ee.FeatureCollection(features)

    # 2. Carregar o dataset SRTM (Altitude)
    srtm = ee.Image("USGS/SRTMGL1_003")

    # 3. Calcular a Declividade (Slope) em Graus
    slope = ee.Terrain.slope(srtm)

    # 4. Combinar Altitude e Declividade em uma única imagem de 2 bandas
    topo_image = srtm.addBands(slope).select(
        ['elevation', 'slope'],          # Bandas originais
        ['elevation', 'slope_degrees']   # Renomeando para clareza
    )

    # 5. Redução por Região: O GEE vai calcular as estatísticas para cada polígono
    # Usamos um Redutor Combinado (Min, Max, Média)
    stats = topo_image.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.mean(),
            sharedInputs=True
        ),
        scale=30  # Resolução do SRTM é 30 metros por pixel
    )

    # 6. Trazer os resultados do servidor do Google para o Python (formato lista)
    return stats.getInfo()['features']

def mask_s2_clouds(image):
    """
    Filtra nuvens e sombras usando a banda SCL (Scene Classification Layer)
    do Sentinel-2 SR.
    """
    scl = image.select('SCL')
    
    # Valores do SCL para descartar:
    # 3: Cloud Shadows (Sombra de nuvem)
    # 8: Cloud Medium Probability (Nuvem média prob)
    # 9: Cloud High Probability (Nuvem alta prob)
    # 10: Cirrus (Nuvens finas)
    # 11: Snow (Neve - raro no Brasil, mas bom ter)
    mask = scl.neq(3).And(scl.neq(8)).And(scl.neq(9)).And(scl.neq(10)).And(scl.neq(11))
    
    return image.updateMask(mask)

def get_ndvi_stats(features_list, start_date, end_date):
    # 1. Preparar a coleção de fazendas
    features = [
        ee.Feature(ee.Geometry(f['geometry']), {'property_id': f['property_id']})
        for f in features_list
    ]
    fc = ee.FeatureCollection(features)

    # 2. Buscar Coleção Sentinel-2
    s2_col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(fc)
                .filterDate(start_date, end_date)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30)) # Filtro rigoroso
                .map(mask_s2_clouds))

    # --- VERIFICAÇÃO DE SEGURANÇA ---
    # Verificamos se a coleção tem pelo menos uma imagem
    count = s2_col.size().getInfo()
    
    if count == 0:
        logging.warning("⚠️ Nenhuma imagem Sentinel-2 limpa encontrada no período.")
        # Retornamos uma lista vazia para que a DAG saiba que não há dados de NDVI
        return []

    # 3. Calcular NDVI e reduzir pela Mediana
    def add_ndvi(img):
        ndvi = img.normalizedDifference(['B8', 'B4']).rename('ndvi')
        return img.addBands(ndvi)

    median_ndvi = s2_col.map(add_ndvi).select('ndvi').median()

    # 4. Redução por Região
    stats = median_ndvi.reduceRegions(
        collection=fc,
        reducer=ee.Reducer.mean().combine(
            reducer2=ee.Reducer.minMax(),
            sharedInputs=True
        ),
        scale=10 
    )

    return stats.getInfo()['features']
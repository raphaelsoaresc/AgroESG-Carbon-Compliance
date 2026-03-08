import os
import pandas as pd
import json
import ee
import google.auth
from datetime import datetime, timedelta
from google.cloud import bigquery

# Importamos as funções do seu próprio repositório
from utils.gee_handler import get_topography_stats, get_ndvi_stats

# Puxa o projeto da exata mesma variável de ambiente que suas DAGs usam
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

if not PROJECT_ID:
    raise ValueError("❌ A variável de ambiente GCP_PROJECT_ID não foi encontrada no seu devenv.")

def authenticate_gee_local():
    """Autentica no GEE garantindo o projeto correto do seu .env"""
    print("🔑 Autenticando no Google Cloud e Earth Engine...")
    try:
        credentials, _ = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/earthengine", 
                "https://www.googleapis.com/auth/cloud-platform"
            ]
        )
        # Inicializa o GEE forçando o uso do projeto da variável de ambiente
        ee.Initialize(credentials=credentials, project=PROJECT_ID)
        print(f"🛰️ Autenticado com sucesso no GEE usando o projeto: {PROJECT_ID}")
    except Exception as e:
        print(f"❌ Erro de autenticação local: {e}")
        raise

def process_failed_queue(csv_path):
    authenticate_gee_local()
    df_failed = pd.read_csv(csv_path)
    
    # Inicia o BigQuery apontando automaticamente para o projeto do .env
    client = bigquery.Client(project=PROJECT_ID)
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

    for index, row in df_failed.iterrows():
        prop_id = row['property_id']
        print(f"Processando {prop_id} ({index+1}/{len(df_failed)})")
        
        # Busca a geometria injetando o PROJECT_ID correto do ambiente
        query = f"""
            SELECT ST_AsGeoJSON(geometry) as geometry_json
            FROM `{PROJECT_ID}.agro_esg_intermediate.int_car_geometries`
            WHERE property_id = '{prop_id}'
        """
        result = list(client.query(query).result())
        if not result:
            print(f"⚠️ Geometria não encontrada para {prop_id}. Pulando...")
            continue
            
        geom = json.loads(result[0]['geometry_json'])
        feature = [{'property_id': prop_id, 'geometry': geom}]
        
        try:
            # Processa no GEE
            topo = get_topography_stats(feature)
            ndvi = get_ndvi_stats(feature, start_date, end_date)
            
            # Mescla os dados
            data = topo[0]['properties']
            if ndvi:
                data.update({
                    'ndvi_mean': ndvi[0]['properties'].get('mean'),
                    'ndvi_min': ndvi[0]['properties'].get('min'),
                    'ndvi_max': ndvi[0]['properties'].get('max')
                })
                
            data['grid_id'] = 'RECOVERY_CSV'
            data['processed_at'] = datetime.now().isoformat()
            
            # Salva no BigQuery injetando a tabela exata via variável
            df_insert = pd.DataFrame([data])
            for col in ['elevation_mean', 'slope_degrees_mean', 'ndvi_mean']:
                if col in df_insert: df_insert[col] = df_insert[col].astype(float)
                
            table_ref = f"{PROJECT_ID}.agro_esg_raw.raw_ee_topography"
            client.load_table_from_dataframe(df_insert, table_ref).result()
            
            print(f"✅ {prop_id} salvo com sucesso no BigQuery!")
            
        except Exception as e:
            print(f"❌ Erro no {prop_id}: {str(e)}")

# --- EXECUÇÃO ---
process_failed_queue('fila_satelite_falha.csv')
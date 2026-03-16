import json
import logging
import io
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Função auxiliar para dividir listas em lotes (chunks)
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# --- CONFIGURAÇÕES ---
default_args = {
    'owner': 'AgroESG',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'satellite_ground_truth_pipeline',
    default_args=default_args,
    description='Monitoramento de Relevo e Vegetação via GEE com Resiliência',
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
    tags=['satellite', 'gee', 'bigquery', 'medallion'],
) as dag:

    # --- TAREFA 1: O FILTRO INTELIGENTE (INCREMENTAL) ---
    @task
    def get_grids_to_process():
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = hook.project_id
        dataset_id = "agro_esg_raw"
        table_id = "raw_ee_topography"

        table_exists = hook.table_exists(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id
        )

        if table_exists:
            logging.info("Tabela destino encontrada. Rodando modo incremental...")
            sql = f"""
                SELECT DISTINCT t1.grid_id 
                FROM agro_esg_intermediate.int_car_grid_mapping t1
                LEFT JOIN `{project_id}.{dataset_id}.{table_id}` t2 
                    ON t1.grid_id = t2.grid_id
                WHERE t2.grid_id IS NULL
                LIMIT 1000
            """
        else:
            logging.info("Tabela destino não existe. Iniciando processamento do zero...")
            sql = """
                SELECT DISTINCT grid_id 
                FROM agro_esg_intermediate.int_car_grid_mapping
                LIMIT 1000
            """
        
        records = hook.get_pandas_df(sql, dialect='standard')
        return records['grid_id'].tolist() if not records.empty else[]

    # --- TAREFA 2: O PROCESSADOR (GEE -> GCS) ---
    @task(pool='gee_api_pool', max_active_tis_per_dag=10)
    def process_grid_satellite(grid_id: str):
        logging.info(f"Iniciando missão de satélite (Relevo + NDVI) para o {grid_id}")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        # 🚨 CORREÇÃO 1: ST_Simplify reduz drasticamente o tamanho do JSON sem perder precisão útil
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        sql = f"""
            SELECT 
                t1.property_id, 
                ST_AsGeoJSON(ST_Simplify(t2.geometry, 5)) as geometry_json
            FROM agro_esg_intermediate.int_car_grid_mapping t1
            JOIN agro_esg_intermediate.int_car_geometries t2 ON t1.property_id = t2.property_id
            WHERE t1.grid_id = '{grid_id}'
        """
        df_geoms = bq_hook.get_pandas_df(sql, dialect='standard')
        
        rows =[]

        if df_geoms.empty:
            logging.warning(f"Grid {grid_id} sem geometrias válidas. Criando registro fantasma.")
            rows.append({
                'property_id': 'GRID_VAZIO',
                'grid_id': grid_id,
                'elevation_min': None, 'elevation_max': None, 'elevation_mean': None,
                'slope_degrees_min': None, 'slope_degrees_max': None, 'slope_degrees_mean': None,
                'ndvi_mean': None, 'ndvi_min': None, 'ndvi_max': None,
                'processed_at': datetime.now().isoformat(),
                'ndvi_start_date': start_date,
                'ndvi_end_date': end_date
            })
        else:
            features_list = [
                {'property_id': r['property_id'], 'geometry': json.loads(r['geometry_json'])}
                for _, r in df_geoms.iterrows()
            ]

            from utils.gee_handler import initialize_gee, get_topography_stats, get_ndvi_stats
            initialize_gee()
            
            topo_results = []
            ndvi_results =[]
            
            # 🚨 CORREÇÃO 2: Processamento em Lotes (Chunks) de 30 para evitar o erro de 10MB
            CHUNK_SIZE = 30
            logging.info(f"Processando {len(features_list)} fazendas em lotes de {CHUNK_SIZE}...")
            
            for chunk in chunk_list(features_list, CHUNK_SIZE):
                try:
                    t_res = get_topography_stats(chunk)
                    if t_res: topo_results.extend(t_res)
                except Exception as e:
                    logging.error(f"Erro no GEE (Topografia) para lote do grid {grid_id}: {e}")
                
                try:
                    n_res = get_ndvi_stats(chunk, start_date, end_date)
                    if n_res: ndvi_results.extend(n_res)
                except Exception as e:
                    logging.error(f"Erro no GEE (NDVI) para lote do grid {grid_id}: {e}")

            # 🚨 CORREÇÃO 3: Nova lógica de fusão. Garante que TODAS as propriedades do grid 
            # recebam uma linha, mesmo que o GEE tenha falhado para elas. Isso destrava a fila.
            topo_map = {res['properties']['property_id']: res['properties'] for res in topo_results} if topo_results else {}
            ndvi_map = {res['properties']['property_id']: res['properties'] for res in ndvi_results} if ndvi_results else {}

            for feat in features_list:
                prop_id = feat['property_id']
                
                data = {
                    'property_id': prop_id,
                    'grid_id': grid_id,
                    'processed_at': datetime.now().isoformat(),
                    'ndvi_start_date': start_date,
                    'ndvi_end_date': end_date
                }
                
                # Mescla Topografia
                if prop_id in topo_map:
                    topo_data = {k: v for k, v in topo_map[prop_id].items() if k not in data}
                    data.update(topo_data)
                else:
                    data.update({
                        'elevation_min': None, 'elevation_max': None, 'elevation_mean': None,
                        'slope_degrees_min': None, 'slope_degrees_max': None, 'slope_degrees_mean': None,
                    })
                    
                # Mescla NDVI
                if prop_id in ndvi_map:
                    data.update({
                        'ndvi_mean': ndvi_map[prop_id].get('mean'),
                        'ndvi_min': ndvi_map[prop_id].get('min'),
                        'ndvi_max': ndvi_map[prop_id].get('max')
                    })
                else:
                    data.update({'ndvi_mean': None, 'ndvi_min': None, 'ndvi_max': None})
                    
                rows.append(data)

        df_results = pd.DataFrame(rows)
        
        # 5. Correção de Tipos
        cols_to_fix =[
            'elevation_min', 'elevation_max', 'elevation_mean',
            'slope_degrees_min', 'slope_degrees_max', 'slope_degrees_mean',
            'ndvi_mean', 'ndvi_min', 'ndvi_max'
        ]
        for col in cols_to_fix:
            if col in df_results.columns:
                df_results[col] = pd.to_numeric(df_results[col], errors='coerce').astype(float)

        # 6. Upload para o GCS
        bucket_name = os.getenv("GCP_BUCKET_NAME", "agro-esg-bronze")
        destination_path = f"satellite/combined_metrics/{grid_id}.parquet"
        
        parquet_buffer = io.BytesIO()
        df_results.to_parquet(parquet_buffer, index=False)
        
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_path, data=parquet_buffer.getvalue())
        
        return f"gs://{bucket_name}/{destination_path}"

    # --- TAREFA 3: O CARREGADOR (GCS -> BIGQUERY) ---
    # 🚨 CORREÇÃO 4: trigger_rule='all_done' garante que a carga rode mesmo se algum grid falhar criticamente
    @task(trigger_rule='all_done')
    def load_satellite_to_bq(file_paths: list):
        # Filtra apenas caminhos válidos (ignora Nones ou Exceptions de tasks que falharam)
        valid_paths =[p for p in file_paths if isinstance(p, str)]
        
        if not valid_paths:
            logging.info("Nenhum arquivo novo para carregar.")
            return None

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = bq_hook.project_id
        dataset_id = "agro_esg_raw"
        table_id = "raw_ee_topography"

        job_config = {
            "load": {
                "sourceUris": valid_paths, 
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        }

        logging.info(f"Carregando {len(valid_paths)} arquivos no BigQuery...")
        bq_hook.insert_job(configuration=job_config)
        return "Carga Finalizada"

    # --- DEFINIÇÃO DO FLUXO ---
    grids = get_grids_to_process()
    processed_files = process_grid_satellite.expand(grid_id=grids)
    load_status = load_satellite_to_bq(processed_files)
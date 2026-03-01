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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Configurações de Resiliência
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
    description='Monitoramento de Relevo e Vegetação via GEE',
    schedule_interval=timedelta(minutes=30),
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
        return records['grid_id'].tolist() if not records.empty else []

    # --- TAREFA 2: O PROCESSADOR (GEE -> GCS) ---
    @task(pool='gee_api_pool', max_active_tis_per_dag=8)
    def process_grid_satellite(grid_id: str):
        logging.info(f"Iniciando missão de satélite (Relevo + NDVI) para o {grid_id}")
        
        # 1. Datas para o NDVI (Janela de 90 dias)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        # 2. Buscar Geometrias no BigQuery
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        sql = f"""
            SELECT t1.property_id, ST_AsGeoJSON(t2.geometry) as geometry_json
            FROM agro_esg_intermediate.int_car_grid_mapping t1
            JOIN agro_esg_intermediate.int_car_geometries t2 ON t1.property_id = t2.property_id
            WHERE t1.grid_id = '{grid_id}'
        """
        df_geoms = bq_hook.get_pandas_df(sql, dialect='standard')
        if df_geoms.empty: return None

        features_list = [
            {'property_id': r['property_id'], 'geometry': json.loads(r['geometry_json'])}
            for _, r in df_geoms.iterrows()
        ]

        # 3. Autenticar e Processar no GEE
        from utils.gee_handler import initialize_gee, get_topography_stats, get_ndvi_stats
        initialize_gee()
        
        logging.info(f"Processando Relevo (SRTM) para {len(features_list)} fazendas...")
        topo_results = get_topography_stats(features_list)
        
        logging.info(f"Processando Vegetação (NDVI 90 dias) para {len(features_list)} fazendas...")
        ndvi_results = get_ndvi_stats(features_list, start_date, end_date)

        # 4. Fusão dos Dados com Renomeação de Segurança
        ndvi_map = {res['properties']['property_id']: res['properties'] for res in ndvi_results}

        rows = []
        for res in topo_results:
            prop_id = res['properties']['property_id']
            data = res['properties'] # Contém elevation_mean, slope, etc.
            
            # APLICAÇÃO DO AJUSTE DE SEGURANÇA (RENOMEAÇÃO LIMPA)
            if prop_id in ndvi_map:
                ndvi_raw = ndvi_map[prop_id]
                ndvi_clean = {
                    'ndvi_mean': ndvi_raw.get('mean'),
                    'ndvi_min': ndvi_raw.get('min'),
                    'ndvi_max': ndvi_raw.get('max')
                }
                data.update(ndvi_clean)
            else:
                # Garante que as chaves existam mesmo se o NDVI falhar para esta prop
                data.update({'ndvi_mean': None, 'ndvi_min': None, 'ndvi_max': None})
            
            data['grid_id'] = grid_id
            data['processed_at'] = datetime.now().isoformat()
            data['ndvi_start_date'] = start_date
            data['ndvi_end_date'] = end_date
            rows.append(data)

        if not rows: return None
        
        df_results = pd.DataFrame(rows)
        
        # 5. Correção de Tipos (Floats) para evitar erro de esquema no BigQuery
        cols_to_fix = [
            'elevation_min', 'elevation_max', 'elevation_mean',
            'slope_degrees_min', 'slope_degrees_max', 'slope_degrees_mean',
            'ndvi_mean', 'ndvi_min', 'ndvi_max'
        ]
        for col in cols_to_fix:
            if col in df_results.columns:
                df_results[col] = pd.to_numeric(df_results[col], errors='coerce').astype(float)

        # 6. Upload para o GCS (Parquet preserva tipos melhor que CSV)
        bucket_name = os.getenv("GCP_BUCKET_NAME", "agro-esg-bronze")
        destination_path = f"satellite/combined_metrics/{grid_id}.parquet"
        
        parquet_buffer = io.BytesIO()
        df_results.to_parquet(parquet_buffer, index=False)
        
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_path, data=parquet_buffer.getvalue())
        
        return f"gs://{bucket_name}/{destination_path}"

    # --- TAREFA 3: O CARREGADOR (GCS -> BIGQUERY) ---
    @task
    def load_satellite_to_bq(file_paths: list):
        valid_paths = [p for p in file_paths if p is not None]
        
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

    # --- TAREFA 4: DISPARAR TRANSFORMAÇÃO DBT ---
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False,
        reset_dag_run=True
    )

    # --- DEFINIÇÃO DO FLUXO ---
    grids = get_grids_to_process()
    processed_files = process_grid_satellite.expand(grid_id=grids)
    load_status = load_satellite_to_bq(processed_files)
    
    load_status >> trigger_dbt_transformation
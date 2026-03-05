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
    'satellite_app_auditor_pipeline',
    default_args=default_args,
    description='Auditoria Cirúrgica de NDVI em APPs Hídricas (Versão Robusta)',
    schedule_interval=timedelta(minutes=30),
    max_active_runs=3,
    catchup=False,
    tags=['satellite', 'gee', 'app', 'compliance', 'medallion'],
) as dag:

    # --- TAREFA 1: FILTRO INCREMENTAL SEGURO ---
    @task
    def get_grids_to_process():
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = hook.project_id
        dataset_id = "agro_esg_raw"
        table_id = "raw_ee_app_ndvi"

        # Verifica se a tabela existe em vez de usar try/except no SQL
        table_exists = hook.table_exists(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id
        )

        if table_exists:
            logging.info(f"Tabela {table_id} encontrada. Filtrando grids não processados...")
            sql = f"""
                SELECT DISTINCT t1.grid_id 
                FROM `{project_id}.agro_esg_intermediate.int_car_grid_mapping` t1
                LEFT JOIN `{project_id}.{dataset_id}.{table_id}` t2 
                    ON t1.grid_id = t2.grid_id
                WHERE t2.grid_id IS NULL
                LIMIT 500
            """
        else:
            logging.info(f"Tabela {table_id} não existe. Processando carga total...")
            sql = f"""
                SELECT DISTINCT grid_id 
                FROM `{project_id}.agro_esg_intermediate.int_car_grid_mapping` 
                LIMIT 500
            """
        
        records = hook.get_pandas_df(sql, dialect='standard')
        return records['grid_id'].tolist() if not records.empty else []

    # --- TAREFA 2: PROCESSADOR ESPACIAL (GEE -> GCS) ---
    @task(pool='gee_api_pool', max_active_tis_per_dag=12) # Reduzido para 10 para evitar concorrência excessiva
    def process_app_satellite(grid_id: str):
        logging.info(f"Iniciando Auditoria de APP para o grid {grid_id}")
        
        # Janela temporal
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = bq_hook.project_id

        # SQL Espacial Robusto
        sql = f"""
            WITH app_zones AS (
                SELECT geometry FROM `{project_id}.agro_esg_intermediate.int_brazil_reference_geometries` 
                WHERE restriction_type = 'APP_ZONE'
            ),
            prop_geoms AS (
                SELECT t1.property_id, t2.geometry
                FROM `{project_id}.agro_esg_intermediate.int_car_grid_mapping` t1
                JOIN `{project_id}.agro_esg_intermediate.int_car_geometries` t2 ON t1.property_id = t2.property_id
                WHERE t1.grid_id = '{grid_id}'
            )
            SELECT 
                p.property_id, 
                ST_AsGeoJSON(ST_UNION_AGG(ST_INTERSECTION(p.geometry, a.geometry))) as app_geometry_json
            FROM prop_geoms p
            INNER JOIN app_zones a ON ST_INTERSECTS(p.geometry, a.geometry)
            GROUP BY 1
        """
        
        df_geoms = bq_hook.get_pandas_df(sql, dialect='standard')
        if df_geoms.empty: 
            logging.warning(f"Nenhuma intersecção de APP encontrada para o grid {grid_id}")
            return None

        # Preparação para o GEE
        app_features_list = []
        for _, r in df_geoms.iterrows():
            geom_dict = json.loads(r['app_geometry_json'])
            if geom_dict and geom_dict.get('coordinates'):
                app_features_list.append({'property_id': r['property_id'], 'geometry': geom_dict})

        if not app_features_list: return None

        # Integração GEE
        from utils.gee_handler import initialize_gee, get_ndvi_stats
        initialize_gee()
        
        logging.info(f"Calculando NDVI para {len(app_features_list)} polígonos de APP...")
        ndvi_results = get_ndvi_stats(app_features_list, start_date, end_date)
        
        if not ndvi_results: 
            logging.error(f"GEE não retornou resultados para o grid {grid_id}")
            return None

        # Formatação e Metadados
        rows = []
        for res in ndvi_results:
            rows.append({
                'property_id': res['properties']['property_id'],
                'grid_id': grid_id,
                'app_ndvi_mean': res['properties'].get('mean'),
                'app_ndvi_min': res['properties'].get('min'),
                'app_ndvi_max': res['properties'].get('max'),
                'processed_at': datetime.now().isoformat(),
                'analysis_start_date': start_date,
                'analysis_end_date': end_date
            })

        df_results = pd.DataFrame(rows)

        # CORREÇÃO DE TIPOS (Crucial para o BigQuery não rejeitar o Parquet)
        cols_to_fix = ['app_ndvi_mean', 'app_ndvi_min', 'app_ndvi_max']
        for col in cols_to_fix:
            df_results[col] = pd.to_numeric(df_results[col], errors='coerce').astype(float)

        # Upload para GCS
        bucket_name = os.getenv("GCP_BUCKET_NAME", "agro-esg-bronze")
        destination_path = f"satellite/app_metrics/{grid_id}.parquet"
        
        parquet_buffer = io.BytesIO()
        df_results.to_parquet(parquet_buffer, index=False)
        
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_path, data=parquet_buffer.getvalue())
        
        return f"gs://{bucket_name}/{destination_path}"

    # --- TAREFA 3: CARREGADOR (GCS -> BIGQUERY) ---
    @task
    def load_app_to_bq(file_paths: list):
        valid_paths = [p for p in file_paths if p is not None]
        if not valid_paths:
            logging.info("Nenhum dado novo para carregar no BigQuery.")
            return None

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        job_config = {
            "load": {
                "sourceUris": valid_paths, 
                "destinationTable": {
                    "projectId": bq_hook.project_id,
                    "datasetId": "agro_esg_raw",
                    "tableId": "raw_ee_app_ndvi",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        }
        
        logging.info(f"Carregando {len(valid_paths)} arquivos Parquet no BigQuery...")
        bq_hook.insert_job(configuration=job_config)
        return "Carga Finalizada"

    # --- TAREFA 4: TRIGGER DBT ---
    trigger_dbt_transformation = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False,
        reset_dag_run=True
    )

    # Fluxo de Execução
    grids = get_grids_to_process()
    processed_files = process_app_satellite.expand(grid_id=grids)
    load_status = load_app_to_bq(processed_files)
    
    load_status >> trigger_dbt_transformation
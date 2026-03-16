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
    description='Auditoria de NDVI em APPs - Versão com LEFT JOIN para Cobertura 100%',
    schedule="@continuous",  # Roda continuamente, mas o filtro incremental garante que só processe novos grids
    max_active_runs=1,
    catchup=False,
    tags=['satellite', 'gee', 'app', 'compliance'],
) as dag:

    @task
    def get_grids_to_process():
        hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = hook.project_id
        dataset_id = "agro_esg_raw"
        table_id = "raw_ee_app_ndvi"

        table_exists = hook.table_exists(project_id=project_id, dataset_id=dataset_id, table_id=table_id)

        if table_exists:
            # CORREÇÃO: Faz o JOIN pelo property_id, mas retorna o grid_id.
            # Assim, se faltar UMA fazenda no grid, o grid inteiro é retornado para reprocessamento.
            # (O BigQuery vai sobrescrever/duplicar as que já existem, mas garante os 100%)
            sql = f"""
                SELECT DISTINCT t1.grid_id 
                FROM `{project_id}.agro_esg_intermediate.int_car_grid_mapping` t1
                LEFT JOIN `{project_id}.{dataset_id}.{table_id}` t2 
                    ON t1.property_id = t2.property_id
                WHERE t2.property_id IS NULL
                LIMIT 1000
            """
        else:
            sql = f"""
                SELECT DISTINCT grid_id 
                FROM `{project_id}.agro_esg_intermediate.int_car_grid_mapping` 
                LIMIT 1000
            """

        records = hook.get_pandas_df(sql, dialect='standard')
        return records['grid_id'].tolist() if not records.empty else []

    @task(pool='gee_api_pool', max_active_tis_per_dag=10)
    def process_app_satellite(grid_id: str):
        logging.info(f"Iniciando Auditoria de APP para o grid {grid_id}")
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        project_id = bq_hook.project_id

        # SQL Espacial - MUDANÇA PARA LEFT JOIN
        # Isso traz todas as propriedades do grid, cruzando ou não com APP
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
            LEFT JOIN app_zones a ON ST_INTERSECTS(p.geometry, a.geometry)
            GROUP BY 1
        """
        
        df_geoms = bq_hook.get_pandas_df(sql, dialect='standard')
        rows = []

        if not df_geoms.empty:
            app_features_list = []
            
            # Separa quem tem APP de quem não tem
            for _, r in df_geoms.iterrows():
                geom_str = r['app_geometry_json']
                
                # Se a fazenda cruza com APP (JSON válido e não nulo)
                if pd.notna(geom_str) and geom_str != 'null':
                    geom_dict = json.loads(geom_str)
                    if geom_dict and geom_dict.get('coordinates'):
                        app_features_list.append({'property_id': r['property_id'], 'geometry': geom_dict})
                    else:
                        # Caso o ST_INTERSECTION resulte em algo vazio
                        rows.append({
                            'property_id': r['property_id'], 'grid_id': grid_id,
                            'app_ndvi_mean': None, 'app_ndvi_min': None, 'app_ndvi_max': None,
                            'processed_at': datetime.now().isoformat(),
                            'analysis_start_date': start_date, 'analysis_end_date': end_date,
                            'status': 'NO_APP_INTERSECTION'
                        })
                else:
                    # Fazenda SEM APP: Adicionamos direto com NDVI nulo
                    rows.append({
                        'property_id': r['property_id'],
                        'grid_id': grid_id,
                        'app_ndvi_mean': None, 'app_ndvi_min': None, 'app_ndvi_max': None,
                        'processed_at': datetime.now().isoformat(),
                        'analysis_start_date': start_date, 'analysis_end_date': end_date,
                        'status': 'NO_APP_INTERSECTION'
                    })

            # Processa no GEE apenas as que têm APP
            if app_features_list:
                from utils.gee_handler import initialize_gee, get_ndvi_stats
                initialize_gee()
                try:
                    logging.info(f"Calculando NDVI para {len(app_features_list)} propriedades com APP no grid {grid_id}")
                    ndvi_results = get_ndvi_stats(app_features_list, start_date, end_date)
                    if ndvi_results:
                        for res in ndvi_results:
                            rows.append({
                                'property_id': res['properties']['property_id'],
                                'grid_id': grid_id,
                                'app_ndvi_mean': res['properties'].get('mean'),
                                'app_ndvi_min': res['properties'].get('min'),
                                'app_ndvi_max': res['properties'].get('max'),
                                'processed_at': datetime.now().isoformat(),
                                'analysis_start_date': start_date, 'analysis_end_date': end_date,
                                'status': 'SUCCESS'
                            })
                except Exception as e:
                    logging.error(f"Erro no GEE para {grid_id}: {str(e)}")
                    for feat in app_features_list:
                        rows.append({
                            'property_id': feat['property_id'], 'grid_id': grid_id,
                            'app_ndvi_mean': None, 'app_ndvi_min': None, 'app_ndvi_max': None,
                            'processed_at': datetime.now().isoformat(),
                            'analysis_start_date': start_date, 'analysis_end_date': end_date,
                            'status': 'GEE_ERROR'
                        })

        # Se o grid estiver totalmente vazio no mapeamento
        if not rows:
            logging.warning(f"Grid {grid_id} sem propriedades mapeadas.")
            rows.append({
                'property_id': 'GRID_VAZIO',
                'grid_id': grid_id,
                'app_ndvi_mean': None, 'app_ndvi_min': None, 'app_ndvi_max': None,
                'processed_at': datetime.now().isoformat(),
                'analysis_start_date': start_date, 'analysis_end_date': end_date,
                'status': 'EMPTY_GRID'
            })

        df_results = pd.DataFrame(rows)

        # Correção de tipos para evitar erros no Parquet/BigQuery
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

    @task
    def load_app_to_bq(file_paths: list):
        if not file_paths or all(p is None for p in file_paths):
            logging.info("Nenhum arquivo para carregar.")
            return None

        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        job_config = {
            "load": {
                "sourceUris": [p for p in file_paths if p], 
                "destinationTable": {
                    "projectId": bq_hook.project_id,
                    "datasetId": "agro_esg_raw",
                    "tableId": "raw_ee_app_ndvi",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "schemaUpdateOptions": ["ALLOW_FIELD_ADDITION"] 
            }
        }
        
        bq_hook.insert_job(configuration=job_config)
        return "Carga Finalizada"

    # Fluxo
    grids = get_grids_to_process()
    processed_files = process_app_satellite.expand(grid_id=grids)
    load_app_to_bq(processed_files)
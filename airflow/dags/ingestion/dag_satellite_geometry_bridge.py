import os
import time
import logging
import ee
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# --- CONFIGURAÇÕES GERAIS ---
# Lê estritamente do .env. Se não existir, quebra a DAG na hora (Fail Fast).
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
if not BUCKET_NAME:
    raise ValueError("🚨 ERRO CRÍTICO: A variável de ambiente GCP_BUCKET_NAME não está definida no .env!")

DATASET_ID = "agro_esg_intermediate"
TABLE_ID = "int_satellite_targets"

default_args = {
    'owner': 'AgroESG',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'satellite_geometry_bridge',
    default_args=default_args,
    description='Ponte Serverless: Exporta Alvos do BQ para GCS e ingere como Asset no GEE',
    schedule_interval="@once", # Ou None para rodar apenas manualmente
    catchup=False,
    tags=['satellite', 'gee', 'bigquery', 'bridge'],
) as dag:

    # ------------------------------------------------------------------------
    # TAREFA 1: EXPORTAÇÃO BQ -> GCS (O Salto para a Nuvem)
    # ------------------------------------------------------------------------
    export_bq_to_gcs = BigQueryToGCSOperator(
        task_id='export_bq_to_gcs',
        source_project_dataset_table=f"{DATASET_ID}.{TABLE_ID}",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/satellite/targets/targets_{{{{ ds_nodash }}}}_*.csv"],
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
        gcp_conn_id='google_cloud_default'
    )

    # ------------------------------------------------------------------------
    # TAREFA 2: INGESTÃO GCS -> GEE (Criando o Mapa Interno)
    # ------------------------------------------------------------------------
    @task
    def ingest_to_gee(ds_nodash: str):
        hook = GoogleBaseHook(gcp_conn_id='google_cloud_default')
        credentials = hook.get_credentials()
        project_id = hook.project_id
        
        ee.Initialize(credentials=credentials, project=project_id)
        logging.info(f"🛰️ GEE autenticado no projeto: {project_id}")

        # 1. Usa o GCSHook para listar os arquivos reais gerados pelo BigQuery
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        prefix = f"satellite/targets/targets_{ds_nodash}_"
        
        files_in_gcs = gcs_hook.list(bucket_name=BUCKET_NAME, prefix=prefix)
        
        if not files_in_gcs:
            raise Exception(f"Nenhum arquivo encontrado no bucket {BUCKET_NAME} com prefixo {prefix}")
            
        # 2. Monta a lista de URIs exatas
        exact_uris =[f"gs://{BUCKET_NAME}/{f}" for f in files_in_gcs]
        logging.info(f"Arquivos encontrados para ingestão ({len(exact_uris)}): {exact_uris}")

        asset_id = f"projects/{project_id}/assets/agro_esg/targets_{ds_nodash}"

        # 3. Passa a lista exata para o Manifesto
        # O GEE exige que cada arquivo CSV seja um "source" separado no manifesto
        sources_list = [{"uris": [uri], "charset": "UTF-8"} for uri in exact_uris]

        manifest = {
            "name": asset_id,
            "sources": sources_list
        }

        gee_task_id = ee.data.newTaskId()[0]
        logging.info(f"Enviando requisição para o GEE...")
        
        # CAPTURA A RESPOSTA DO GOOGLE PARA PEGAR O ID REAL
        response = ee.data.startTableIngestion(gee_task_id, manifest)
        
        # Extrai o ID real que o Google gerou para podermos monitorar
        real_task_id = response.get('name', gee_task_id).split('/')[-1]
        
        logging.info(f"✅ Ingestão aceita! Task ID Real: {real_task_id}")
        logging.info(f"Destino do Asset: {asset_id}")
        
        return real_task_id

    # ------------------------------------------------------------------------
    # TAREFA 3: SENSOR DE PRONTIDÃO (O Garçom)
    # ------------------------------------------------------------------------
    @task
    def wait_for_gee_ingestion(gee_task_id: str):
        hook = GoogleBaseHook(gcp_conn_id='google_cloud_default')
        ee.Initialize(credentials=hook.get_credentials(), project=hook.project_id)
        
        logging.info(f"Monitorando a tarefa do GEE: {gee_task_id}")
        
        while True:
            status = ee.data.getTaskStatus(gee_task_id)[0]
            state = status['state']
            
            if state == 'COMPLETED':
                logging.info("✅ Ingestão concluída com sucesso! Asset pronto para uso.")
                break
            elif state in ['FAILED', 'CANCELLED']:
                error_msg = status.get('error_message', 'Erro desconhecido no GEE')
                logging.error(f"❌ Falha na ingestão do GEE: {error_msg}")
                raise Exception(f"GEE Task Failed: {error_msg}")
            else:
                logging.info(f"Status atual: {state}... aguardando 30 segundos.")
                time.sleep(30)

    # --- ORQUESTRAÇÃO DO FLUXO ---
    gee_task_id = ingest_to_gee(ds_nodash="{{ ds_nodash }}")
    
    export_bq_to_gcs >> gee_task_id
    wait_for_gee_ingestion(gee_task_id)
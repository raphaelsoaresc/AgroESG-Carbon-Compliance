import os
import hashlib
import zipfile
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID")
TABLE_ID = "sigef_history"

RAW_PATH = os.getenv("RAW_PATH_SIGEF")
STAGING_PATH = os.getenv("STAGING_PATH")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_SIGEF")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_geo_file_with_duckdb(ti):
    # 1. Busca o arquivo na pasta RAW
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Diretório RAW não encontrado: {RAW_PATH}")

    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.zip', '.shp'))]
    if not files:
        raise FileNotFoundError("Nenhum arquivo do SIGEF encontrado.")
    
    original_filename = files[0]
    full_path = os.path.join(RAW_PATH, original_filename)
    
    # 2. Gera o Hash do arquivo ORIGINAL para auditoria
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    extracted_files_list = []
    working_path = full_path
    
    # 3. Tratamento de ZIP (Extração para ler o Shapefile interno)
    if original_filename.endswith('.zip'):
        print(f"Arquivo ZIP detectado: {original_filename}. Extraindo...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            extracted_files_list = zip_ref.namelist()
            zip_ref.extractall(RAW_PATH)
            
            # No SIGEF, buscamos o arquivo .shp dentro do ZIP
            shps = [f for f in extracted_files_list if f.endswith('.shp')]
            if shps:
                working_path = os.path.join(RAW_PATH, shps[0])
            else:
                raise ValueError(f"O ZIP {original_filename} não contém um Shapefile (.shp) válido.")

    # 4. Definição do Arquivo de Saída
    output_filename = f"sigef_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # 5. Execução da Query (Convertendo GEOM para texto para o BigQuery)
    query = f"""
        COPY (
            SELECT 
                * EXCLUDE (geom), 
                ST_AsText(geom) as geom,
                '{file_hash}' as file_hash,
                '{original_filename}' as source_filename,
                now() as ingested_at
            FROM st_read('{working_path}')
        ) TO '{output_path}' (FORMAT 'PARQUET');
    """
    
    print(f"Executando no DuckDB: {query}")
    con.execute(query)
    con.close()

    # 6. Faxina: Remove arquivos extraídos (mantém a pasta raw limpa para o move funcionar direito)
    # Nota: Removemos apenas os arquivos extraídos, mantendo o ZIP original para ser arquivado
    if original_filename.endswith('.zip') and extracted_files_list:
        print("🧹 Limpando arquivos extraídos temporários...")
        for f in extracted_files_list:
            file_to_remove = os.path.join(RAW_PATH, f)
            if os.path.exists(file_to_remove):
                os.remove(file_to_remove)
    
    # 7. Retorno para o Airflow
    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='original_file', value=original_filename)

with DAG(
    'ingestion_sigef_to_bronze', 
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['bronze', 'sigef', 'duckdb'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_sigef_file',
        filepath=RAW_PATH,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,           
        mode='reschedule'          
    )

    process_file = PythonOperator(
        task_id='process_with_duckdb',
        python_callable=process_geo_file_with_duckdb
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}"),
        dst="bronze/sigef/{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}",
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
        execution_timeout=timedelta(minutes=10),
        mime_type='application/octet-stream'
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_to_bigquery_bronze',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/bronze/sigef/{{{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}}}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_ID,
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND",
                "autodetect": True,
            }
        },
        do_xcom_push=False
    )

    archive_original = BashOperator(
        task_id='archive_original_file',
        bash_command="""
            FILE_NAME="{{ ti.xcom_pull(task_ids='process_with_duckdb', key='original_file') }}";
            FILE_BASE="${FILE_NAME%.*}";
            ARCHIVE_DIR="{{ params.archive_path }}/$(date +%Y%m%d)";
            
            mkdir -p "$ARCHIVE_DIR" && \
            mv -f "{{ params.raw_path }}/${FILE_BASE}".* "$ARCHIVE_DIR/"
        """,
        params={
            'raw_path': RAW_PATH,
            'archive_path': ARCHIVE_PATH
        }
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False,
        reset_dag_run=True
    )

    wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original >> trigger_dbt
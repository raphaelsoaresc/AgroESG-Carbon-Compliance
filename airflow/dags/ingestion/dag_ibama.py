import os
import hashlib
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

# --- CONFIGURAÇÕES ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID")
TABLE_ID = "ibama_history"

RAW_PATH = os.getenv("RAW_PATH_IBAMA")
STAGING_PATH = os.getenv("STAGING_PATH")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_IBAMA")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_ibama_file_with_duckdb(ti):
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.csv', '.zip', '.shp'))]
    if not files:
        raise FileNotFoundError("Nenhum arquivo do IBAMA encontrado.")
    
    file_name = files[0]
    full_path = os.path.join(RAW_PATH, file_name)
    
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    output_filename = f"ibama_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    
    if file_name.endswith('.csv') or (file_name.endswith('.zip') and 'ibama' in file_name.lower()):
        read_cmd = f"""read_csv_auto('{full_path}', 
                        ALL_VARCHAR=TRUE, 
                        HEADER=TRUE,
                        STRICT_MODE=FALSE,
                        IGNORE_ERRORS=TRUE)"""
        select_clause = "*"
    else:
        # Mantemos o ST_AsText para Shapefiles (SIGEF/IBAMA Geo)
        con.execute("INSTALL spatial; LOAD spatial;")
        read_cmd = f"st_read('{full_path}')"
        select_clause = "* EXCLUDE (geom), ST_AsText(geom) as geom"

    query = f"""
        COPY (
            SELECT 
                {select_clause},
                '{file_hash}' as file_hash,
                '{file_name}' as source_filename,
                now() as ingested_at
            FROM {read_cmd}
        ) TO '{output_path}' (FORMAT 'PARQUET');
    """
    con.execute(query)
    con.close()
    
    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='original_file', value=file_name)

with DAG(
    'ingestion_ibama_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'ibama'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_ibama_file',
        filepath=RAW_PATH,
        fs_conn_id='fs_default',
        poke_interval=30
    )

    process_file = PythonOperator(
        task_id='process_ibama_with_duckdb',
        python_callable=process_ibama_file_with_duckdb
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_ibama_to_gcs',
        src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='process_ibama_with_duckdb', key='output_filename') }}"),
        dst="bronze/ibama/{{ ti.xcom_pull(task_ids='process_ibama_with_duckdb', key='output_filename') }}",
        bucket=BUCKET_NAME,
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_ibama_to_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/bronze/ibama/{{{{ ti.xcom_pull(task_ids='process_ibama_with_duckdb', key='output_filename') }}}}"],
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

    archive_file = BashOperator(
        task_id='archive_ibama_file',
        bash_command=f"mkdir -p {ARCHIVE_PATH}/$(date +%Y%m%d) && mv {RAW_PATH}/{{{{ ti.xcom_pull(task_ids='process_ibama_with_duckdb', key='original_file') }}}} {ARCHIVE_PATH}/$(date +%Y%m%d)/"
    )

    wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_file

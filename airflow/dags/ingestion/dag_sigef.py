import os
import hashlib
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

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
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.zip', '.shp'))]
    if not files:
        raise FileNotFoundError("Nenhum arquivo encontrado para processar.")
    
    file_name = files[0]
    full_path = os.path.join(RAW_PATH, file_name)
    
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    output_filename = f"sigef_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # O segredo: Converter GEOM para texto para o BigQuery carregar como STRING na Bronze
    query = f"""
        COPY (
            SELECT 
                * EXCLUDE (geom), 
                ST_AsText(geom) as geom,
                '{file_hash}' as file_hash,
                '{file_name}' as source_filename,
                now() as ingested_at
            FROM st_read('{full_path}')
        ) TO '{output_path}' (FORMAT 'PARQUET');
    """
    con.execute(query)
    con.close()
    
    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='original_file', value=file_name)

with DAG(
    'ingestion_sigef_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'sigef', 'duckdb'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_sigef_file',
        filepath=RAW_PATH,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600
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
    )

    # Operador Robusto para Airflow 2.10.0
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
        bash_command=(
            f"FILE_NAME=\"{{{{ ti.xcom_pull(task_ids='process_with_duckdb', key='original_file') }}}}\"; "
            "FILE_BASE=\"${FILE_NAME%.*}\"; "  # Extrai o nome sem a extensão
            f"mkdir -p {ARCHIVE_PATH}/$(date +%Y%m%d) && "
            f"mv {RAW_PATH}/${{FILE_BASE}}.* {ARCHIVE_PATH}/$(date +%Y%m%d)/"
        )
    )

    wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original

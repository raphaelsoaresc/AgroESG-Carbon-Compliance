import os
import hashlib
import zipfile
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor  # <--- CORREÇÃO AQUI
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES (Padrão solicitado)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = "agro_esg_raw"
STAGING_PATH = "./data/staging"

SOURCES = {
    'ibge': {
        'table_id': "ibge_biomes",
        'raw_path': "./data/raw/ibge",
        'archive_path': "./data/archive/ibge"
    },
    'funai': {
        'table_id': "funai_terras_indigenas",
        'raw_path': "./data/raw/funai",
        'archive_path': "./data/archive/funai"
    },
    'incra': {
        'table_id': "incra_quilombolas",
        'raw_path': "./data/raw/incra",
        'archive_path': "./data/archive/incra"
    }
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para o PythonSensor verificar se existe arquivo .zip ou .shp
def check_for_files(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    files = [f for f in os.listdir(path) if f.endswith(('.zip', '.shp'))]
    return len(files) > 0

def process_reference_geo_file(source_key, ti):
    conf = SOURCES[source_key]
    raw_path = conf['raw_path']
    extracted_files_list = []
    
    files = [f for f in os.listdir(raw_path) if f.endswith(('.zip', '.shp'))]
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {raw_path}")
    
    original_filename = files[0]
    full_path = os.path.join(raw_path, original_filename)
    
    try:
        with open(full_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        
        working_path = full_path
        if original_filename.endswith('.zip'):
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                extracted_files_list = zip_ref.namelist()
                zip_ref.extractall(raw_path)
                shps = [f for f in extracted_files_list if f.endswith('.shp')]
                if shps:
                    working_path = os.path.join(raw_path, shps[0])
                else:
                    raise ValueError("ZIP sem Shapefile válido.")

        output_filename = f"{source_key}_{file_hash}.parquet"
        output_path = os.path.join(STAGING_PATH, output_filename)
        
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL spatial; LOAD spatial;")
        
        query = f"""
            COPY (
                SELECT 
                    * EXCLUDE (geom), 
                    ST_AsText(ST_Transform(geom, 'EPSG:4674', 'EPSG:4326')) as geom,
                    '{file_hash}' as file_hash,
                    '{original_filename}' as source_filename,
                    now() as ingested_at
                FROM st_read('{working_path}')
            ) TO '{output_path}' (FORMAT 'PARQUET');
        """
        con.execute(query)
        con.close()
        
        ti.xcom_push(key='output_filename', value=output_filename)
        ti.xcom_push(key='output_path', value=output_path)
        ti.xcom_push(key='original_file', value=original_filename)

    finally:
        if extracted_files_list:
            for f in extracted_files_list:
                temp_file = os.path.join(raw_path, f)
                if os.path.exists(temp_file) and temp_file != full_path:
                    os.remove(temp_file)

with DAG(
    'ingestion_reference_brazil_to_bronze', 
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['bronze', 'reference', 'duckdb'],
) as dag:

    task_groups = []

    for source_id, config in SOURCES.items():
        with TaskGroup(group_id=f'group_{source_id}') as tg:
            
            # 1. Definimos o ID da tarefa de processamento para usar no XCom
            # Isso evita confusão com as aspas dentro do Jinja
            process_task_id = f"group_{source_id}.process_{source_id}_with_duckdb"
            
            wait_for_file = PythonSensor(
                task_id=f'wait_for_{source_id}_file',
                python_callable=check_for_files,
                op_kwargs={'path': config['raw_path']},
                poke_interval=30,
                timeout=600,
                mode='reschedule'
            )

            process_file = PythonOperator(
                task_id=f'process_{source_id}_with_duckdb',
                python_callable=process_reference_geo_file,
                op_kwargs={'source_key': source_id}
            )

            # 2. Upload GCS: Usando concatenação simples (+) em vez de f-string com chaves quádruplas
            upload_to_gcs = LocalFilesystemToGCSOperator(
                task_id=f'upload_{source_id}_parquet_to_gcs',
                src="{{ ti.xcom_pull(task_ids='" + process_task_id + "', key='output_path') }}",
                dst=f"bronze/reference/{source_id}/" + "{{ ti.xcom_pull(task_ids='" + process_task_id + "', key='output_filename') }}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default'
            )

            # 3. Load BQ: Corrigindo a URI que estava vindo com "}}" no final
            load_to_bq = BigQueryInsertJobOperator(
                task_id=f'load_{source_id}_to_bigquery_bronze',
                configuration={
                    "load": {
                        "sourceUris": [
                            f"gs://{BUCKET_NAME}/bronze/reference/{source_id}/" + 
                            "{{ ti.xcom_pull(task_ids='" + process_task_id + "', key='output_filename') }}"
                        ],
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET_ID,
                            "tableId": config['table_id'],
                        },
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_TRUNCATE",
                        "autodetect": True,
                    }
                }
            )

            # 4. Archive: Limpando a sintaxe do Bash
            archive_original = BashOperator(
                task_id=f'archive_{source_id}_original_file',
                bash_command="""
                    FILE_NAME="{{ ti.xcom_pull(task_ids='""" + process_task_id + """', key='original_file') }}";
                    PARQUET_LOCAL="{{ ti.xcom_pull(task_ids='""" + process_task_id + """', key='output_path') }}";
                    ARCHIVE_DIR="{{ params.archive_path }}/$(date +%Y%m%d)";
                    
                    mkdir -p "$ARCHIVE_DIR";
                    mv -f "{{ params.raw_path }}/$FILE_NAME" "$ARCHIVE_DIR/";
                    rm -f "$PARQUET_LOCAL";
                """,
                params={
                    'raw_path': config['raw_path'],
                    'archive_path': config['archive_path']
                }
            )

            wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original
            task_groups.append(tg)

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False,
        reset_dag_run=True
    )

    task_groups >> trigger_dbt
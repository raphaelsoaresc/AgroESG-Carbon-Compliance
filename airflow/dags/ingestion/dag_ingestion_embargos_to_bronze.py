import os
import hashlib
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor # <-- IMPORT ALTERADO AQUI
from airflow.utils.task_group import TaskGroup
import zipfile

# --- CONFIGURAÇÕES GERAIS ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID")
STAGING_PATH = os.getenv("STAGING_PATH", "./data/staging")

# --- MAPEAMENTO DAS FONTES ---
SOURCES = {
    'ibama': {
        'table_id': "ibama_history",
        'raw_path': "./data/raw/ibama",
        'archive_path': "./data/archive/ibama"
    },
    'sema_mt': {
        'table_id': "sema_mt_embargos",
        'raw_path': "./data/raw/sema_embargos",
        'archive_path': "./data/archive/sema_embargos"
    },
    'siga_mt': {
        'table_id': "siga_mt_embargos",
        'raw_path': "./data/raw/siga_embargos",
        'archive_path': "./data/archive/siga_embargos"
    },
    'icmbio': {
        'table_id': "icmbio_embargos",
        'raw_path': "./data/raw/icmbio_embargos",
        'archive_path': "./data/archive/icmbio_embargos"
    }
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- NOVA FUNÇÃO DO SENSOR ---
def check_for_valid_files(raw_path, **kwargs):
    """Verifica fisicamente se existe um zip, csv ou shp na pasta"""
    if not os.path.exists(raw_path):
        print(f"Aguardando... Diretório ainda não existe: {raw_path}")
        return False
        
    files =[f for f in os.listdir(raw_path) if f.endswith(('.csv', '.zip', '.shp'))]
    
    if files:
        print(f"Arquivo encontrado! Liberando a DAG. Arquivos: {files}")
        return True
        
    print(f"Aguardando... Nenhum arquivo válido (.zip, .csv, .shp) em: {raw_path}")
    return False

# --- FUNÇÃO CORE DE PROCESSAMENTO (DUCKDB) ---
def process_embargo_file_with_duckdb(source_name, raw_path, **kwargs):
    ti = kwargs['ti']
    
    files =[f for f in os.listdir(raw_path) if f.endswith(('.csv', '.zip', '.shp'))]
    original_filename = files[0]
    full_path = os.path.join(raw_path, original_filename)
    
    # Gera Hash MD5 do arquivo original
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    working_path = full_path 
    extracted_files_list =[]
    
    # Lógica de extração se for ZIP
    if original_filename.endswith('.zip'):
        print(f"Arquivo ZIP detectado: {original_filename}. Extraindo...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            extracted_files_list = zip_ref.namelist()
            zip_ref.extractall(raw_path)
            
            csvs =[f for f in extracted_files_list if f.endswith('.csv')]
            shps = [f for f in extracted_files_list if f.endswith('.shp')]
            
            if csvs:
                working_path = os.path.join(raw_path, csvs[0])
            elif shps:
                working_path = os.path.join(raw_path, shps[0])
            else:
                raise ValueError(f"O ZIP {original_filename} não contém CSV nem SHP válidos.")

    output_filename = f"{source_name}_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    
    # Lógica de Leitura e Tratamento Espacial
    if working_path.endswith('.csv'):
        read_cmd = f"""read_csv_auto('{working_path}', 
                        ALL_VARCHAR=TRUE, 
                        HEADER=TRUE,
                        NORMALIZE_NAMES=TRUE,
                        QUOTE='"',
                        IGNORE_ERRORS=TRUE)"""
        select_clause = "*"
    else:
        con.execute("INSTALL spatial; LOAD spatial;")
        read_cmd = f"st_read('{working_path}')"
        # Converte SIRGAS 2000 (4674) para WGS 84 (4326) e corrige geometrias inválidas
        select_clause = "* EXCLUDE (geom), ST_AsText(ST_MakeValid(ST_Transform(geom, 'EPSG:4674', 'EPSG:4326'))) as geom"

    # Query de cópia para Parquet
    query = f"""
        COPY (
            SELECT 
                {select_clause},
                '{file_hash}' as file_hash,
                '{original_filename}' as source_filename,
                now() as ingested_at
            FROM {read_cmd}
        ) TO '{output_path}' (FORMAT 'PARQUET');
    """
    
    print(f"Executando no DuckDB: {query}")
    con.execute(query)
    con.close()

    # Limpeza dos arquivos extraídos do ZIP
    if extracted_files_list:
        print("🧹 Limpando arquivos extraídos temporários...")
        for f in extracted_files_list:
            file_to_remove = os.path.join(raw_path, f)
            if os.path.exists(file_to_remove) and file_to_remove != full_path:
                os.remove(file_to_remove)
    
    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='original_file', value=original_filename)


# --- DEFINIÇÃO DA DAG ---
with DAG(
    'ingestion_embargos_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['bronze', 'embargos', 'duckdb', 'compliance'],
) as dag:

    for source_name, config in SOURCES.items():
        
        with TaskGroup(group_id=source_name) as source_group:
            
            # SENSOR SUBSTITUÍDO AQUI
            wait_for_file = PythonSensor(
                task_id='wait_for_file',
                python_callable=check_for_valid_files,
                op_kwargs={'raw_path': config['raw_path']},
                poke_interval=30,
                timeout=600,
                mode='reschedule'
            )

            process_file = PythonOperator(
                task_id='process_with_duckdb',
                python_callable=process_embargo_file_with_duckdb,
                op_kwargs={
                    'source_name': source_name,
                    'raw_path': config['raw_path']
                },
                provide_context=True
            )

            upload_to_gcs = LocalFilesystemToGCSOperator(
                task_id='upload_parquet_to_gcs',
                src=os.path.join(STAGING_PATH, f"{{{{ ti.xcom_pull(task_ids='{source_name}.process_with_duckdb', key='output_filename') }}}}"),
                dst=f"bronze/embargos/{source_name}/{{{{ ti.xcom_pull(task_ids='{source_name}.process_with_duckdb', key='output_filename') }}}}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default',
                execution_timeout=timedelta(minutes=10),
                mime_type='application/octet-stream'
            )

            load_to_bq = BigQueryInsertJobOperator(
                task_id='load_to_bq',
                configuration={
                    "load": {
                        "sourceUris":[f"gs://{BUCKET_NAME}/bronze/embargos/{source_name}/{{{{ ti.xcom_pull(task_ids='{source_name}.process_with_duckdb', key='output_filename') }}}}"],
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET_ID,
                            "tableId": config['table_id'],
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
                    f"FILE_NAME=\"{{{{ ti.xcom_pull(task_ids='{source_name}.process_with_duckdb', key='original_file') }}}}\"; "
                    f"SRC_FILE=\"{config['raw_path']}/$FILE_NAME\"; "
                    f"DEST_DIR=\"{config['archive_path']}/$(date +%Y%m%d)\"; "
                    f"mkdir -p \"$DEST_DIR\" && "
                    f"if [ -f \"$SRC_FILE\" ]; then "
                    f"  mv -f \"$SRC_FILE\" \"$DEST_DIR/\"; "
                    f"else "
                    f"  echo \"Arquivo $SRC_FILE não encontrado, assumindo já movido.\"; "
                    f"fi"
                )
            )

            wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original
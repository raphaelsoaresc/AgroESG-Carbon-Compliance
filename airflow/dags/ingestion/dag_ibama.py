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
import zipfile

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
    # 1. Busca o arquivo na pasta RAW
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.csv', '.zip', '.shp'))]
    if not files:
        raise FileNotFoundError("Nenhum arquivo do IBAMA encontrado.")
    
    original_filename = files[0]
    full_path = os.path.join(RAW_PATH, original_filename)
    
    # 2. Gera o Hash do arquivo ORIGINAL (seja ZIP, CSV ou SHP) para auditoria
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    # 3. Tratamento de ZIP (A correção do erro)
    # Se for ZIP, extraímos para garantir que o DuckDB leia o arquivo real
    working_path = full_path # Por padrão, o caminho de trabalho é o arquivo original
    
    if original_filename.endswith('.zip'):
        print(f"Arquivo ZIP detectado: {original_filename}. Extraindo...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(RAW_PATH)
            
            # Procura o que foi extraído
            extracted_files = zip_ref.namelist()
            
            # Tenta achar um CSV
            csvs = [f for f in extracted_files if f.endswith('.csv')]
            # Tenta achar um SHP
            shps = [f for f in extracted_files if f.endswith('.shp')]
            
            if csvs:
                working_path = os.path.join(RAW_PATH, csvs[0])
                print(f"CSV extraído e selecionado: {working_path}")
            elif shps:
                working_path = os.path.join(RAW_PATH, shps[0])
                print(f"Shapefile extraído e selecionado: {working_path}")
            else:
                raise ValueError(f"O ZIP {original_filename} não contém CSV nem SHP válidos.")

    # 4. Definição do Arquivo de Saída
    output_filename = f"ibama_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    
    # 5. Lógica de Leitura (Preservando o que funcionou no teste local)
    # Verifica a extensão do arquivo DE TRABALHO (já extraído), não do original
    if working_path.endswith('.csv'):
        # ATUALIZAÇÃO CRÍTICA:
        # 1. QUOTE='"': Diz pro DuckDB que o ; dentro de aspas é texto, não coluna.
        # 2. IGNORE_ERRORS=TRUE: Se tiver uma linha podre, ele ignora e segue a vida (essencial para Bronze).
        read_cmd = f"""read_csv_auto('{working_path}', 
                        ALL_VARCHAR=TRUE, 
                        HEADER=TRUE,
                        NORMALIZE_NAMES=TRUE,
                        QUOTE='"',
                        IGNORE_ERRORS=TRUE)"""
        select_clause = "*"
    else:
        # LÓGICA PRESERVADA PARA SHAPEFILES (Não mexi aqui):
        con.execute("INSTALL spatial; LOAD spatial;")
        read_cmd = f"st_read('{working_path}')"
        select_clause = "* EXCLUDE (geom), ST_AsText(geom) as geom"

    # 6. Execução da Query
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
    
    # 7. Retorno para o Airflow
    ti.xcom_push(key='output_filename', value=output_filename)
    # Importante: Passamos o arquivo ORIGINAL para o BashOperator mover para archive
    ti.xcom_push(key='original_file', value=original_filename)

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

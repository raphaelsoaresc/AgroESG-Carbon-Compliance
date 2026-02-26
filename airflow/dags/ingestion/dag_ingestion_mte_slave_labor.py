import os
import hashlib
from datetime import datetime, timedelta
import duckdb
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID", "agro_esg_raw")

TABLE_ID = "mte_slave_labor_history"

# Usando abspath para evitar o bug do FileSensor procurando em /./data/...
RAW_PATH = os.path.abspath(os.getenv("RAW_PATH_MTE_SLAVE_LABOR", "./data/raw/mte"))
STAGING_PATH = os.getenv("STAGING_PATH", "./data/staging")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_MTE_SLAVE_LABOR", "./data/archive/mte_slave_labor")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def discover_encoding(filepath):
    """
    Descobre o encoding do arquivo dinamicamente.
    Zero hardcode, previne quebras se o MTE modernizar o formato amanhã.
    """
    for enc in ['utf-8', 'latin-1', 'windows-1252']:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                f.read(8192)  # Lê um chunk de 8KB só para testar os bytes
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'  # Fallback padrão mundial

def process_mte_file_with_duckdb(**kwargs):
    ti = kwargs['ti']

    # 1. Busca o arquivo na pasta RAW
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Diretório RAW não encontrado: {RAW_PATH}")
        
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.csv', '.zip', '.shp'))]
    if not files:
        raise FileNotFoundError("Nenhum arquivo do MTE encontrado.")

    original_filename = files[0]
    full_path = os.path.join(RAW_PATH, original_filename)
    
    # 2. Gera o Hash do arquivo ORIGINAL
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
        
    # 3. Tratamento de ZIP
    working_path = full_path 
    extracted_files_list = []
    
    if original_filename.endswith('.zip'):
        print(f"Arquivo ZIP detectado: {original_filename}. Extraindo...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            extracted_files_list = zip_ref.namelist()
            zip_ref.extractall(RAW_PATH)
            
            csvs = [f for f in extracted_files_list if f.endswith('.csv')]
            shps = [f for f in extracted_files_list if f.endswith('.shp')]
            
            if csvs:
                working_path = os.path.join(RAW_PATH, csvs[0])
            elif shps:
                working_path = os.path.join(RAW_PATH, shps[0])
            else:
                raise ValueError(f"O ZIP {original_filename} não contém CSV nem SHP válidos.")
                
    # 4. Definição do Arquivo de Saída
    output_filename = f"mte_slave_labor_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)

    con = duckdb.connect(database=':memory:')
    
    # 5. Lógica de Leitura Blindada
    clean_csv_path = None
    if working_path.endswith('.csv'):
        clean_csv_path = working_path.replace('.csv', '_utf8_limpo.csv')
        
        # O Python lê o arquivo no formato do Windows (cp1252), 
        # substitui qualquer lixo irreconhecível por '?' (errors='replace')
        # e salva um arquivo novo, garantido ser 100% UTF-8.
        with open(working_path, 'r', encoding='cp1252', errors='replace') as f_in:
            with open(clean_csv_path, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    f_out.write(line)
        
        # O DuckDB agora lê o arquivo limpo, sabendo que o delimitador é ';'
        read_cmd = f"""read_csv_auto('{clean_csv_path}', 
                        NORMALIZE_NAMES=TRUE,
                        ALL_VARCHAR=TRUE,
                        DELIM=';',
                        HEADER=TRUE,
                        NULL_PADDING=TRUE,
                        QUOTE='"')"""
        select_clause = "*"
    else:
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
    
    # Apaga o arquivo _utf8_limpo.csv que o Python gerou
    if clean_csv_path and os.path.exists(clean_csv_path):
        os.remove(clean_csv_path)
    # LIMPEZA
    if extracted_files_list:
        print("🧹 Limpando arquivos extraídos temporários...")
        for f in extracted_files_list:
            file_to_remove = os.path.join(RAW_PATH, f)
            if os.path.exists(file_to_remove) and file_to_remove != full_path:
                os.remove(file_to_remove)
                
    # 7. Retorno para o Airflow (XCom)
    print(f"Enviando XComs -> output_filename: {output_filename}, original_file: {original_filename}")
    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='original_file', value=original_filename)

with DAG(
    'ingestion_mte_slave_labor',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=False,
    tags=['bronze', 'mte', 'slave_labor', 'duckdb'],
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_mte_file',
        filepath=RAW_PATH,
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    process_file = PythonOperator(
        task_id='process_mte_with_duckdb',
        python_callable=process_mte_file_with_duckdb,
        provide_context=True 
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='process_mte_with_duckdb', key='output_filename') }}"),
        dst="bronze/mte_slave_labor/{{ ti.xcom_pull(task_ids='process_mte_with_duckdb', key='output_filename') }}",
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
        execution_timeout=timedelta(minutes=10),
        mime_type='application/octet-stream'
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id='load_mte_to_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/bronze/mte_slave_labor/{{{{ ti.xcom_pull(task_ids='process_mte_with_duckdb', key='output_filename') }}}}"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_ID,
                },
                "sourceFormat": "PARQUET",
                # IMPORTANTE: WRITE_TRUNCATE temporário para limpar o esquema cagado
                "writeDisposition": "WRITE_APPEND", 
                "autodetect": True,
            }
        },
        do_xcom_push=False
    )

    archive_original = BashOperator(
        task_id='archive_original_file',
        bash_command=(
            f"FILE_NAME=\"{{{{ ti.xcom_pull(task_ids='process_mte_with_duckdb', key='original_file') }}}}\"; "
            f"SRC_FILE=\"{RAW_PATH}/$FILE_NAME\"; "
            f"DEST_DIR=\"{ARCHIVE_PATH}/$(date +%Y%m%d)\"; "
            f"mkdir -p \"$DEST_DIR\" && "
            f"if [ -f \"$SRC_FILE\" ]; then "
            f"  mv -f \"$SRC_FILE\" \"$DEST_DIR/\"; "
            f"else "
            f"  echo \"Arquivo $SRC_FILE não encontrado, assumindo já movido.\"; "
            f"fi"
        )
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt_transformation',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False,
        reset_dag_run=True
    )

    wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original >> trigger_dbt
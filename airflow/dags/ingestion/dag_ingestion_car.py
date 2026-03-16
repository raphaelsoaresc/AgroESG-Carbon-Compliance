import os
import hashlib
import duckdb
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES GLOBAIS ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID", "agro_esg_raw")

RAW_PATH = os.getenv("RAW_PATH_CAR", "./data/raw/car")
STAGING_PATH = os.getenv("STAGING_PATH", "./data/staging")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_CAR", "./data/archive/car")

# --- CONFIGURAÇÕES DOS ESTADOS ---
STATES = ['PA', 'MT', 'AM', 'RO']

# Caixas delimitadoras (BBOX) aproximadas para cada estado. 
# Isso evita que polígonos com erro de digitação (ex: lat/long 0,0) quebrem análises futuras.
STATE_BBOXES = {
    'PA': 'ST_MakeEnvelope(-58.89, -9.84, -46.06, 2.59)',
    'MT': 'ST_MakeEnvelope(-61.63, -18.04, -50.22, -7.34)',
    'AM': 'ST_MakeEnvelope(-73.80, -9.81, -56.09, 2.24)',
    'RO': 'ST_MakeEnvelope(-66.80, -13.69, -59.77, -7.96)'
}

# --- GERAÇÃO DINÂMICA DAS FONTES ---
# Mantemos os CSVs que possivelmente são nacionais ou únicos
CAR_SOURCES = {
    'temas_ambientais': {'file': 'TEMAS_AMBIENTAIS.CSV', 'table': 'car_temas_ambientais', 'type': 'csv'},
    'sobreposicao': {'file': 'SOBREPOSICAO.CSV', 'table': 'car_sobreposicao', 'type': 'csv'},
    'metadados_api': {'file': 'METADADOS_API_CPF_CNPJ.csv', 'table': 'car_metadados_proprietarios', 'type': 'csv'}
}

# Adicionamos dinamicamente os arquivos espaciais para cada estado
for uf in STATES:
    CAR_SOURCES[f'area_imovel_{uf.lower()}'] = {
        'file': f'AREA_IMOVEL_{uf}.zip', # IMPORTANTE: Renomeie seus zips na pasta raw para este padrão
        'table': f'car_area_imovel_geometria_{uf.lower()}', # Tabela separada por estado no BQ
        'type': 'spatial',
        'uf': uf
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_file_exists(file_pattern):
    if not os.path.exists(RAW_PATH): return False
    return any(file_pattern in f for f in os.listdir(RAW_PATH))

def process_car_with_duckdb(source_key, ti):
    conf = CAR_SOURCES[source_key]
    file_pattern = conf['file']
    
    files =[f for f in os.listdir(RAW_PATH) if file_pattern in f]
    if not files:
        raise FileNotFoundError(f"Arquivo contendo {file_pattern} não encontrado.")
    
    original_filename = files[0]
    full_path = os.path.join(RAW_PATH, original_filename)

    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()
    
    output_filename = f"car_{source_key}_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    con = duckdb.connect(database=':memory:')
    con.execute("SET memory_limit='4GB';") 
    con.execute("SET threads=2;")
    
    extracted_files =[]
    working_path = full_path

    if original_filename.lower().endswith('.zip'):
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(RAW_PATH)
            extracted_files = zip_ref.namelist()
            
            ext = '.shp' if conf['type'] == 'spatial' else '.csv'
            candidates = [f for f in extracted_files if f.endswith(ext)]
            
            if not candidates:
                raise ValueError(f"Nenhum arquivo {ext} encontrado no ZIP.")
            working_path = os.path.join(RAW_PATH, candidates[0])

    try:
        if conf['type'] == 'spatial':
            uf = conf['uf']
            bbox = STATE_BBOXES[uf]
            con.execute("INSTALL spatial; LOAD spatial;")
            
            # Adicionada a coluna uf_origem e o BBOX dinâmico
            query = f"""
                COPY (
                    SELECT 
                        * EXCLUDE (geom),
                        ST_AsText(ST_MakeValid(geom)) as wkt_geom,
                        '{uf}' as uf_origem,
                        '{file_hash}' as file_hash,
                        '{original_filename}' as source_filename,
                        now() as ingested_at
                    FROM st_read('{working_path}')
                    WHERE ST_Intersects(geom, {bbox})
                ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
            """
        else:
            query = f"""
                COPY (
                    SELECT 
                        *,
                        '{file_hash}' as file_hash,
                        '{original_filename}' as source_filename,
                        now() as ingested_at
                    FROM read_csv_auto('{working_path}', ALL_VARCHAR=TRUE, NORMALIZE_NAMES=TRUE, IGNORE_ERRORS=TRUE)
                ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
            """
        
        print(f"🦆 Executando DuckDB para {source_key}...")
        con.execute(query)
        
    finally:
        con.close()
        for f in extracted_files:
            p = os.path.join(RAW_PATH, f)
            if os.path.exists(p) and p != full_path:
                os.remove(p)

    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='output_path', value=output_path)
    ti.xcom_push(key='original_file', value=original_filename)

with DAG(
    'ingestion_car_to_bronze_optimized',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    max_active_tasks=1, # Mantido em 1 para proteger a RAM de 8GB
    catchup=False,
    tags=['bronze', 'car', 'spatial', 'duckdb'],
) as dag:

    for s_id, s_conf in CAR_SOURCES.items():
        with TaskGroup(group_id=f'group_{s_id}') as tg:
            
            wait = PythonSensor(
                task_id='wait_file',
                python_callable=check_file_exists,
                op_kwargs={'file_pattern': s_conf['file']},
                poke_interval=60,
                timeout=3600,
                mode='reschedule'
            )

            process = PythonOperator(
                task_id='process_duckdb',
                python_callable=process_car_with_duckdb,
                op_kwargs={'source_key': s_id}
            )

            upload = LocalFilesystemToGCSOperator(
                task_id='upload_gcs',
                src="{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_path') }}",
                dst=f"bronze/car/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default'
            )

            load = BigQueryInsertJobOperator(
                task_id='load_bq',
                configuration={
                    "load": {
                        "sourceUris":[f"gs://{BUCKET_NAME}/bronze/car/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}"],
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET_ID,
                            "tableId": s_conf['table']
                        },
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_TRUNCATE", 
                        "autodetect": True,
                    }
                }
            )

            archive = BashOperator(
                task_id='archive',
                bash_command="""
                    DEST="{{ params.arch }}/$(date +%Y%m%d)"
                    mkdir -p "$DEST"
                    FILE="{{ params.raw }}/{{ ti.xcom_pull(task_ids='group_"""+s_id+""".process_duckdb', key='original_file') }}"
                    if [ -f "$FILE" ]; then
                        mv -f "$FILE" "$DEST/"
                    fi
                    rm -f "{{ ti.xcom_pull(task_ids='group_"""+s_id+""".process_duckdb', key='output_path') }}"
                """,
                params={'raw': RAW_PATH, 'arch': ARCHIVE_PATH}
            )

            wait >> process >> upload >> load >> archive

    
    [tg for tg in dag.task_group_dict.values() if isinstance(tg, TaskGroup)]
import os
import hashlib
import duckdb
import zipfile # Adicionado para tratar o ZIP
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES GLOBAIS
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = "agro_esg_raw"
STAGING_PATH = "./data/staging"
MT_BBOX = "ST_MakeEnvelope(-61.63, -18.04, -50.22, -7.34)"

HYDRO_SOURCES = {
    'ana': {
        'table_id': 'ana_app_zones',
        'raw_path': './data/raw/ana',
        'archive_path': './data/archive/ana',
        'layer_name': 'geoft_bho_ach_otto_nivel_07',
        'file_ext': '.gpkg'
    },
    'ibge_bc250': {
        'table_id': 'ibge_bc250_app_zones',
        'raw_path': './data/raw/ibge_bc250',
        'archive_path': './data/archive/ibge_bc250',
        'layer_name': 'hid_massa_dagua_a', 
        'file_ext': '.gpkg'
    }
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_for_files(path, ext):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    # Agora aceita a extensão original OU .zip
    files = [f for f in os.listdir(path) if f.endswith(ext) or f.endswith('.zip')]
    return len(files) > 0

def process_hydro_with_duckdb(source_key, ti):
    conf = HYDRO_SOURCES[source_key]
    raw_path = conf['raw_path']
    
    files = [f for f in os.listdir(raw_path) if f.endswith(conf['file_ext']) or f.endswith('.zip')]
    if not files:
        raise FileNotFoundError(f"Arquivo não encontrado para {source_key}")
    
    original_filename = files[0]
    full_path = os.path.join(raw_path, original_filename)
    
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    output_filename = f"hydro_{source_key}_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    temp_db_path = os.path.join(STAGING_PATH, f"temp_{source_key}_{file_hash}.db")
    
    extracted_files = []
    working_path = full_path

    try:
        # --- TRATAMENTO DE ZIP ---
        if original_filename.endswith('.zip'):
            print(f"📦 Extraindo {original_filename}...")
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(raw_path)
                extracted_files = zip_ref.namelist()
                gpkgs = [f for f in extracted_files if f.endswith('.gpkg')]
                if gpkgs:
                    working_path = os.path.join(raw_path, gpkgs[0])
                else:
                    raise ValueError("Nenhum arquivo .gpkg encontrado dentro do ZIP.")

        con = duckdb.connect(temp_db_path)
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("SET memory_limit='1GB';")
        con.execute("SET threads=1;")

        if source_key == 'ana':
            geom_col = "wts_gm"
            sql_logic = f"""
                SELECT 
                    wts_cd_pfafstetterbasin as cobacia,
                    wts_cd_pfafstetterbasincodelevel as river_order,
                    CASE 
                        WHEN river_order >= 5 THEN 30 
                        WHEN river_order BETWEEN 3 AND 4 THEN 50 
                        ELSE 100 
                    END as app_width_m,
                    ST_AsText(ST_Transform(ST_Simplify(ST_Buffer(ST_Transform({geom_col}, 'EPSG:4674', 'EPSG:5880'), app_width_m), 30), 'EPSG:5880', 'EPSG:4326')) as wkt_geom_app
            """
        else:
            # Lógica para IBGE BC250 - Trocado rowid por row_number() OVER ()
            geom_col = "geom" 
            sql_logic = f"""
                SELECT 
                    CAST(row_number() OVER () AS VARCHAR) as cobacia, 
                    0 as river_order,
                    30 as app_width_m,
                    ST_AsText(ST_Transform(ST_Simplify(ST_Buffer(ST_Transform({geom_col}, 'EPSG:4674', 'EPSG:5880'), 30), 30), 'EPSG:5880', 'EPSG:4326')) as wkt_geom_app
            """

        query = f"""
            COPY (
                WITH filtered AS (
                    SELECT * FROM st_read('{working_path}', layer='{conf['layer_name']}')
                    WHERE ST_Intersects({geom_col}, {MT_BBOX})
                )
                {sql_logic},
                '{file_hash}' as file_hash,
                now() as ingested_at
                FROM filtered
            ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """

        print(f"🦆 Processando {source_key}...")
        con.execute(query)
        
        # Check de integridade
        res = con.execute(f"SELECT count(*) FROM read_parquet('{output_path}')").fetchone()
        print(f"✅ Sucesso: {res[0]} polígonos gerados.")

    finally:
        if 'con' in locals(): con.close()
        if os.path.exists(temp_db_path): os.remove(temp_db_path)
        # Limpeza de arquivos extraídos
        for f in extracted_files:
            p = os.path.join(raw_path, f)
            if os.path.exists(p) and p != full_path:
                os.remove(p)

    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='output_path', value=output_path)
    ti.xcom_push(key='original_file', value=original_filename)

with DAG(
    'ingestion_brazil_hydro_to_bronze',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=['bronze', 'hydro', 'spatial'],
) as dag:

    for s_id, s_conf in HYDRO_SOURCES.items():
        with TaskGroup(group_id=f'group_{s_id}') as tg:
            
            wait = PythonSensor(
                task_id='wait_file',
                python_callable=check_for_files,
                op_kwargs={'path': s_conf['raw_path'], 'ext': s_conf['file_ext']},
                poke_interval=30, timeout=600, mode='reschedule'
            )

            process = PythonOperator(
                task_id='process_duckdb',
                python_callable=process_hydro_with_duckdb,
                op_kwargs={'source_key': s_id}
            )

            upload = LocalFilesystemToGCSOperator(
                task_id='upload_gcs',
                src="{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_path') }}",
                dst=f"bronze/hydro/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default'
            )

            load = BigQueryInsertJobOperator(
                task_id='load_bq',
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{BUCKET_NAME}/bronze/hydro/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}"],
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": s_conf['table_id']},
                        "sourceFormat": "PARQUET", "writeDisposition": "WRITE_TRUNCATE", "autodetect": True,
                    }
                }
            )

            archive = BashOperator(
                task_id='archive',
                bash_command="""
                    mkdir -p "{{ params.arch }}/$(date +%Y%m%d)"
                    mv -f "{{ params.raw }}/{{ ti.xcom_pull(task_ids='group_"""+s_id+""".process_duckdb', key='original_file') }}" "{{ params.arch }}/$(date +%Y%m%d)/"
                    rm -f "{{ ti.xcom_pull(task_ids='group_"""+s_id+""".process_duckdb', key='output_path') }}"
                """,
                params={'raw': s_conf['raw_path'], 'arch': s_conf['archive_path']}
            )

            wait >> process >> upload >> load >> archive

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_transformation_medallion',
        wait_for_completion=False
    )

    tg >> trigger_dbt
import os
import hashlib
import duckdb
import zipfile
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator

# --- CONFIGURAÇÕES GLOBAIS
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = "agro_esg_raw"
STAGING_PATH = "./data/staging"
TEMP_EXTRACT_PATH = "./data/tmp_extract"

# BBOX convertida manualmente para EPSG:3857 (Metros) para a ANA
# Isso evita o erro de transformação no WHERE
ANA_BBOX_3857 = "ST_MakeEnvelope(-8237634, -2049247, -5120707, 591121)"

# BBOX em EPSG:4326 (Graus) para o IBGE
IBGE_BBOX_4326 = "ST_MakeEnvelope(-74.0, -18.1, -46.0, 5.3)"

SPATIAL_SOURCES = {
    'ana_rios_50k': {
        'table_id': 'ana_rios_app',
        'raw_path': './data/raw/ana',
        'archive_path': './data/archive/ana',
        'file_ext': '.zip',
        'logic_type': 'rios_ana',
        'source_crs': 'EPSG:3857',
        'zip_filename': 'Base_Hidrogr%C3%A1fica_Ottocodificada_2017_50K_-_trecho_de_drenagem.zip',
        'bbox_sql': ANA_BBOX_3857
    },
    # --- IBGE COMENTADO (SKIP) ---
    # 'ibge_rodovias': {
    #     'table_id': 'ibge_rodovias_risco',
    #     'raw_path': './data/raw/ibge_bc250',
    #     'archive_path': './data/archive/ibge_bc250',
    #     'file_ext': '.zip',
    #     'logic_type': 'rodovias_ibge',
    #     'source_crs': 'EPSG:4674',
    #     'layer_name': 'rod_trecho_rodoviario_l',
    #     'bbox_sql': IBGE_BBOX_4326
    # }
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_spatial_data(source_key, ti):
    conf = SPATIAL_SOURCES[source_key]
    raw_path = conf['raw_path']
    full_path = os.path.join(raw_path, conf['zip_filename'])
    
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {full_path}")
    
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    output_filename = f"{source_key}_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    task_extract_path = os.path.join(TEMP_EXTRACT_PATH, source_key)
    
    if os.path.exists(task_extract_path):
        shutil.rmtree(task_extract_path)
    os.makedirs(task_extract_path, exist_ok=True)

    try:
        print(f"📦 Extraindo {conf['zip_filename']}...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(task_extract_path)
        
        # Busca o arquivo de dados
        data_file = None
        for root, dirs, files in os.walk(task_extract_path):
            for f in files:
                if f.endswith(('.shp', '.gpkg')):
                    data_file = os.path.join(root, f)
                    break
        
        if not data_file: raise ValueError("Nenhum arquivo .shp ou .gpkg encontrado.")

        con = duckdb.connect(':memory:')
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("SET memory_limit='2GB';")

        # --- LÓGICA SQL ---
        if conf['logic_type'] == 'rios_ana':
            sql_select = f"""
                SELECT 
                    COBACIA as id_origem,
                    'Rio' as categoria,
                    NUSTRAHLER as ordem,
                    CASE 
                        WHEN NUSTRAHLER >= 5 THEN 100 
                        WHEN NUSTRAHLER BETWEEN 3 AND 4 THEN 50 
                        ELSE 30 
                    END as buffer_m,
                    ST_AsText(ST_Transform(ST_Simplify(ST_Buffer(ST_Transform(geom, '{conf['source_crs']}', 'EPSG:5880'), buffer_m), 10), 'EPSG:5880', 'EPSG:4326')) as wkt_geom_app
            """
        elif conf['logic_type'] == 'rodovias_ibge':
            sql_select = f"""
                SELECT 
                    CAST(row_number() OVER () AS VARCHAR) as id_origem,
                    'Rodovia' as categoria,
                    CASE WHEN jurisdicao IN ('Federal', 'Estadual') THEN 40 ELSE 20 END as buffer_m,
                    ST_AsText(ST_Transform(ST_Simplify(ST_Buffer(ST_Transform(geom, '{conf['source_crs']}', 'EPSG:5880'), buffer_m), 10), 'EPSG:5880', 'EPSG:4326')) as wkt_geom_app
            """
        
        # Para Shapefiles, não passamos o parâmetro 'layer'
        layer_clause = f", layer='{conf['layer_name']}'" if data_file.endswith('.gpkg') else ""

        query = f"""
            COPY (
                WITH filtered AS (
                    SELECT * FROM st_read('{data_file}' {layer_clause})
                    WHERE ST_Intersects(geom, {conf['bbox_sql']})
                )
                {sql_select},
                '{file_hash}' as file_hash,
                now() as ingested_at
                FROM filtered
            ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """
        
        print(f"🦆 Executando DuckDB para {source_key}...")
        con.execute(query)
        
        count = con.execute(f"SELECT count(*) FROM read_parquet('{output_path}')").fetchone()[0]
        print(f"✅ Sucesso: {count} linhas geradas.")
        
        if count == 0:
            raise ValueError(f"ERRO: Zero linhas geradas. Verifique se a BBOX {conf['bbox_sql']} está correta para o arquivo.")

    finally:
        if 'con' in locals(): con.close()
        if os.path.exists(task_extract_path): shutil.rmtree(task_extract_path)

    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='output_path', value=output_path)
    ti.xcom_push(key='original_file', value=conf['zip_filename'])

with DAG(
    'ingestion_spatial_compliance_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['agro_esg', 'spatial', 'ana'],
) as dag:

    for s_id, s_conf in SPATIAL_SOURCES.items():
        with TaskGroup(group_id=f'group_{s_id}') as tg:
            
            wait = PythonSensor(
                task_id='wait_file',
                python_callable=lambda path, filename: os.path.exists(os.path.join(path, filename)),
                op_kwargs={'path': s_conf['raw_path'], 'filename': s_conf['zip_filename']},
                poke_interval=30, timeout=600
            )

            process = PythonOperator(
                task_id='process_duckdb',
                python_callable=process_spatial_data,
                op_kwargs={'source_key': s_id}
            )

            upload = LocalFilesystemToGCSOperator(
                task_id='upload_gcs',
                src="{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_path') }}",
                dst=f"bronze/spatial/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default'
            )

            load = BigQueryInsertJobOperator(
                task_id='load_bq',
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{BUCKET_NAME}/bronze/spatial/{s_id}/" + "{{ ti.xcom_pull(task_ids='group_" + s_id + ".process_duckdb', key='output_filename') }}"],
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
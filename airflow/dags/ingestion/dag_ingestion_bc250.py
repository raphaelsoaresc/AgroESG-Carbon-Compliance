import os
import hashlib
import duckdb
import zipfile
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- CONFIGURAÇÕES GLOBAIS
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = "agro_esg_raw"
STAGING_PATH = "./data/staging"
TEMP_EXTRACT_PATH = "./data/tmp_extract"
IBGE_BBOX_4326 = "ST_MakeEnvelope(-74.0, -18.1, -46.0, 5.3)"

BC250_CONFIG = {
    'rodovias': {
        'layer': 'rod_trecho_rodoviario_l',
        'table': 'ibge_bc250_rodovias',
        'columns': "sigla, revestimento, operacional, jurisdicao, administracao, tipovia, situacaofisica"
    },
    'pistas_pouso_l': {
        'layer': 'aer_pista_ponto_pouso_l',
        'table': 'ibge_bc250_pistas_pouso_l',
        'columns': "nome, tipopista, revestimento, usopista, homologacao, operacional, situacaofisica, largura, extensao"
    },
    'energia': {
        'layer': 'enc_trecho_energia_l',
        'table': 'ibge_bc250_linhas_energia',
        'columns': "nome, especie, operacional, situacaofisica, largurafaixaservidao, sin"
    },
    'hidro_poligonos': {
        'layer': 'hid_massa_dagua_a',
        'table': 'ibge_bc250_massas_agua',
        'columns': "nome, tipomassadagua, regime, salgada, dominialidade, artificial"
    },
    'hidro_linhas': {
        'layer': 'hid_trecho_drenagem_l',
        'table': 'ibge_bc250_rios_linhas',
        'columns': "nome, tipotrechodrenagem, navegavel, larguramedia, regime"
    }
}

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0, # Em PC local com pouca RAM, melhor falhar e você ver o que houve
}

def process_bc250_layer(layer_key, ti):
    conf = BC250_CONFIG[layer_key]
    gpkg_path = "./data/raw/ibge_bc250/bc250_2025-11-21.gpkg"
    
    with open(gpkg_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    output_filename = f"bc250_{layer_key}_{file_hash}.parquet"
    output_path = os.path.join(STAGING_PATH, output_filename)
    
    # TRAVA DE SEGURANÇA: Banco em disco para não estourar RAM
    temp_db = f"/tmp/duckdb_{layer_key}.db"
    if os.path.exists(temp_db): os.remove(temp_db)

    try:
        con = duckdb.connect(database=temp_db)
        con.execute("INSTALL spatial; LOAD spatial;")
        
        # TRAVA DE SEGURANÇA: Limite de memória explícito
        con.execute("SET memory_limit='2GB';")
        con.execute("SET max_temp_directory_size='10GB';")
        con.execute(f"SET temp_directory='/tmp/duck_tmp_{layer_key}';")

        print(f"🦆 Processando {layer_key} (Layer: {conf['layer']})...")
        
        query = f"""
            COPY (
                SELECT 
                    {conf['columns']},
                    ST_AsText(ST_Transform(geom, 'EPSG:4674', 'EPSG:4326')) as geometry_wkt,
                    '{file_hash}' as file_hash,
                    now() as ingested_at
                FROM st_read('{gpkg_path}', layer='{conf['layer']}')
                WHERE ST_Intersects(geom, {IBGE_BBOX_4326})
            ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
        """
        con.execute(query)
        
    finally:
        con.close()
        if os.path.exists(temp_db): os.remove(temp_db)

    ti.xcom_push(key='output_filename', value=output_filename)
    ti.xcom_push(key='output_path', value=output_path)

with DAG(
    'ingestion_ibge_bc250_safe_mode',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    # TRAVA DE SEGURANÇA MÁXIMA: Só roda 1 tarefa por vez no PC inteiro
    max_active_tasks=1, 
    tags=['agro_esg', 'low_ram', 'bc250'],
) as dag:

    for layer_id, layer_conf in BC250_CONFIG.items():
        with TaskGroup(group_id=f'group_{layer_id}') as tg:
            
            process = PythonOperator(
                task_id='duckdb_transform',
                python_callable=process_bc250_layer,
                op_kwargs={'layer_key': layer_id}
            )

            upload = LocalFilesystemToGCSOperator(
                task_id='upload_gcs',
                src="{{ ti.xcom_pull(task_ids='group_" + layer_id + ".duckdb_transform', key='output_path') }}",
                dst=f"bronze/ibge_bc250/{layer_id}/" + "{{ ti.xcom_pull(task_ids='group_" + layer_id + ".duckdb_transform', key='output_filename') }}",
                bucket=BUCKET_NAME,
                gcp_conn_id='google_cloud_default'
            )

            load = BigQueryInsertJobOperator(
                task_id='load_bq',
                configuration={
                    "load": {
                        "sourceUris": [f"gs://{BUCKET_NAME}/bronze/ibge_bc250/{layer_id}/*.parquet"],
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": layer_conf['table']},
                        "sourceFormat": "PARQUET", 
                        "writeDisposition": "WRITE_TRUNCATE", 
                        "autodetect": True,
                    }
                }
            )

            process >> upload >> load
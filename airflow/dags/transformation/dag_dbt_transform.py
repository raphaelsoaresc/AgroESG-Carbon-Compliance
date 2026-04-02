import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import duckdb
from dotenv import load_dotenv
import requests
import pyarrow as pa
import gc

# 1. PRIMEIRO definimos o BASE_DIR (Caminho raiz do projeto)
BASE_DIR = Path(__file__).resolve().parents[3]

# 2. DEPOIS carregamos o .env usando o BASE_DIR
env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=env_path)

# 3. AGORA configuramos a conexão com o Cloud Run
raw_url = os.getenv("API_URL")

# Fallback caso o .env não seja lido corretamente
if not raw_url or "localhost" in raw_url:
    raw_url = "https://caipora-sentinela-api-534128993934.us-central1.run.app"

# Limpeza da URL para o Airflow (remove https:// e barras extras)
clean_host = raw_url.replace("https://", "").replace("http://", "").split('/')[0]

# Injeção da conexão no ambiente do Airflow
os.environ["AIRFLOW_CONN_CAIPORA_API_CONN"] = f"http://{clean_host}?schema=https"

# Log para conferência no Airflow
print(f"🚀 URL DA API CONFIGURADA: {raw_url}")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
try:
    from airflow.providers.http.operators.http import SimpleHttpOperator
except ImportError:
    from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely import wkt

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
STAGING_PATH = os.getenv("STAGING_PATH", "/tmp")

BASE_DIR = Path(__file__).resolve().parents[3]
DBT_PROJECT_PATH = BASE_DIR / "agro_credit_transform"
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"
GCP_KEY_PATH = BASE_DIR / "config" / "gcp_credentials.json"
DBT_EXECUTABLE = BASE_DIR / ".devenv" / "state" / "venv" / "bin" / "dbt"

profile_config = ProfileConfig(
    profile_name="agro_credit_transform",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": PROJECT_ID,
            "dataset": "agro_esg_staging",
            "keyfile": str(GCP_KEY_PATH),
        },
    ),
)

# --- FUNÇÃO CORRIGIDA (Encoding + Nova Tabela) ---
def generate_compliance_geoparquet(**kwargs):
    client = bigquery.Client()
    
    db_temp_file = os.path.join(STAGING_PATH, "temp_duckdb.db")
    if os.path.exists(db_temp_file): os.remove(db_temp_file)
    
    con = duckdb.connect(db_temp_file)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    tables = [
        {"id": "fct_compliance_risk", "file": "fct_compliance_latest.parquet"},
        {"id": "fct_compliance_geometries_mart", "file": "fct_compliance_geometries.parquet"}
    ]
    
    for table in tables:
        table_id = f"{PROJECT_ID}.agro_esg_marts.{table['id']}"
        output_path = os.path.join(STAGING_PATH, table['file'])
        
        print(f"🦆 Extraindo {table_id} em lotes para poupar RAM...")

        query_job = client.query(f"SELECT * FROM `{table_id}`")
        rows_iter = query_job.result(page_size=50000)
        
        chunk_count = 0
        first_chunk = True
        batch = []
        
        for row in rows_iter:
            batch.append(dict(row))
            if len(batch) >= 50000:
                chunk_count += 1
                process_and_insert_chunk(con, batch, "tmp_table", first_chunk)
                batch = []
                gc.collect()
                print(f"  ✅ Processados {chunk_count * 50000} registros...")
                first_chunk = False

        if batch:
            process_and_insert_chunk(con, batch, "tmp_table", first_chunk)
            batch = []
            gc.collect()

        print(f"💾 Convertendo para GeoParquet final: {output_path}")
        
        cols_info = con.execute("PRAGMA table_info('tmp_table')").fetchall()
        columns = [c[1] for c in cols_info]
        geom_cols = [c for c in columns if c.startswith('geom') or c == 'geometry']
        
        select_parts = []
        for col in columns:
            if col in geom_cols:
                # A CORREÇÃO ESTÁ AQUI: Adicionado ::VARCHAR para garantir o tipo correto
                select_parts.append(f"CASE WHEN \"{col}\" IS NOT NULL THEN ST_GeomFromText(\"{col}\"::VARCHAR) ELSE NULL END AS \"{col}\"")
            else:
                select_parts.append(f"\"{col}\"")
        
        sql_query = f"SELECT {', '.join(select_parts)} FROM tmp_table"
        
        try:
            con.execute(f"COPY ({sql_query}) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');")
            print(f"✨ Tabela {table['id']} concluída com sucesso!")
        except Exception as e:
            print(f"❌ Erro ao salvar GeoParquet: {e}")
            raise e
        finally:
            con.execute("DROP TABLE IF EXISTS tmp_table;")

    con.close()
    if os.path.exists(db_temp_file): os.remove(db_temp_file)

def process_and_insert_chunk(con, batch, table_name, first_chunk):
    """Função auxiliar para sanitizar e inserir um lote no DuckDB"""
    df = pd.DataFrame(batch)
    
    # Sanitização de Encoding (UTF-8)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(
            lambda x: str(x).encode('utf-8', 'replace').decode('utf-8') if x is not None else None
        )
    
    # Registra o DataFrame no DuckDB
    if first_chunk:
        # Na primeira vez, cria a tabela
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    else:
        # Nas próximas, apenas insere
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

def notify_api_refresh(**kwargs):
    # Pega a URL e a Senha do ambiente
    api_url = os.getenv("API_URL", "https://caipora-sentinela-api-534128993934.us-central1.run.app")
    api_key = os.getenv("API_PASSWORD")
    
    endpoint = f"{api_url.rstrip('/')}/admin/refresh-data"
    
    print(f"🚀 Enviando sinal de refresh para: {endpoint}")
    
    try:
        response = requests.post(
            endpoint, 
            headers={"X-API-Key": api_key},
            timeout=60 # Dá tempo para o Cloud Run "acordar"
        )
        
        print(f"📡 Status Code: {response.status_code}")
        print(f"📄 Resposta: {response.text}")
        
        # Se der erro 4xx ou 5xx, a DAG falha e mostra o porquê
        response.raise_for_status()
        
    except Exception as e:
        print(f"❌ Erro ao notificar API: {str(e)}")
        raise

with DAG(
    dag_id="dbt_transformation_medallion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_tasks=3, 
    max_active_runs=1,
    tags=["dbt", "gold", "api", "geoparquet"],
) as dag:

    # 1. DBT: Adicionado a nova mart no select
    dbt_transform_group = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=str(DBT_PROJECT_PATH),
            manifest_path=str(MANIFEST_PATH),
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT_EXECUTABLE),
            execution_mode=ExecutionMode.LOCAL,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_ALL,
            select=["+fct_compliance_risk", "+fct_compliance_geometries_mart"],
            emit_datasets=False, 
        ),
        operator_args={"install_deps": True},
    )
    
    generate_parquet_task = PythonOperator(
        task_id='generate_compliance_geoparquet',
        python_callable=generate_compliance_geoparquet,
        provide_context=True
    )

    # 3. Upload: Usando wildcard para pegar os dois arquivos gerados
    upload_to_gcs_task = BashOperator(
        task_id='upload_parquet_to_gcs',
        bash_command=f"gsutil -m cp {STAGING_PATH}/*.parquet gs://{BUCKET_NAME}/api_data/"
    )

    refresh_api_task = PythonOperator(
        task_id='refresh_api_data',
        python_callable=notify_api_refresh,
        provide_context=True
    )

    clean_local_file_task = BashOperator(
        task_id='clean_local_file',
        bash_command=f"rm -f {STAGING_PATH}/*.parquet"
    )

    dbt_transform_group >> generate_parquet_task >> upload_to_gcs_task >> refresh_api_task >> clean_local_file_task
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Carregamento do .env (Deve ser um dos primeiros comandos)
from dotenv import load_dotenv
load_dotenv() 

# --- INJEÇÃO DE CONFIGURAÇÕES PARA O TERMINAL ---
# Isso permite que o dag.test() encontre a conexão sem precisar do banco do Airflow
os.environ["AIRFLOW_CONN_CAIPORA_API_CONN"] = os.getenv("API_URL", "http://localhost:8000")

# Imports do Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
try:
    from airflow.providers.http.operators.http import SimpleHttpOperator
except ImportError:
    from airflow.providers.http.operators.http import HttpOperator as SimpleHttpOperator

# Imports Cosmos (dbt)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Imports para processamento de dados
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely import wkt

# --- CONFIGURAÇÕES DE AMBIENTE ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
STAGING_PATH = os.getenv("STAGING_PATH", "/tmp")

# Caminhos do dbt
BASE_DIR = Path(__file__).resolve().parents[3]
DBT_PROJECT_PATH = BASE_DIR / "agro_credit_transform"
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"
GCP_KEY_PATH = BASE_DIR / "config" / "gcp_credentials.json"
DBT_EXECUTABLE = BASE_DIR / ".devenv" / "state" / "venv" / "bin" / "dbt"

# Configuração do Perfil dbt
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

# --- FUNÇÃO PYTHON (Processamento de GeoParquet) ---
def generate_compliance_geoparquet(**kwargs):
    ti = kwargs['ti']
    
    print("🛰️ Iniciando extração do BigQuery para GeoParquet...")
    client = bigquery.Client()
    
    table_id = f"{PROJECT_ID}.agro_esg_marts.fct_compliance_risk"
    query = f"SELECT * FROM `{table_id}`"
    
    df = client.query(query).to_dataframe()
    
    if df.empty:
        raise ValueError(f"A tabela {table_id} está vazia.")

    if 'geometry' in df.columns:
        print("🗺️ Convertendo WKT para Geometria...")
        try:
            # BRECHA 1: Tratamento de nulos para as "Fazendas Invisíveis"
            df['geometry'] = df['geometry'].apply(
                lambda x: wkt.loads(x) if pd.notnull(x) and isinstance(x, str) else None
            )
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
            
            null_geoms = gdf['geometry'].isnull().sum()
            if null_geoms > 0:
                print(f"⚠️ Aviso: {null_geoms} propriedades sem geometria incluídas no arquivo.")
        except Exception as e:
            print(f"Erro na conversão de geometria: {e}")
            raise
    else:
        raise ValueError("Coluna 'geometry' não encontrada na tabela fato.")

    # Padronização de datas para compatibilidade DuckDB/API
    for col in gdf.columns:
        if 'date' in col.lower() or 'timestamp' in col.lower():
            gdf[col] = pd.to_datetime(gdf[col])

    file_name = "fct_compliance_latest.parquet"
    output_path = os.path.join(STAGING_PATH, file_name)
    
    print(f"💾 Salvando GeoParquet em: {output_path}")
    gdf.to_parquet(output_path, index=False, compression='snappy')
    
    ti.xcom_push(key='parquet_filename', value=file_name)

# --- DEFINIÇÃO DA DAG ---
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

    # 1. Transformação dbt (Inclui novas regras de sobreposição e passivos)
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
            select=["+fct_compliance_risk"],
            emit_datasets=False, 
        ),
        # SOLUÇÃO: Passa o argumento direto para o operador, ignorando o construtor do Config
        operator_args={
            "install_deps": True,
        },
    )
    
    # 2. Geração do GeoParquet
    generate_parquet_task = PythonOperator(
        task_id='generate_compliance_geoparquet',
        python_callable=generate_compliance_geoparquet,
        provide_context=True
    )

    # 3. Upload para o Bucket GCS
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}"),
        dst="api_data/{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}",
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
        mime_type='application/octet-stream',
        chunk_size=5 * 1024 * 1024,
        execution_timeout=timedelta(minutes=30),
        retries=3,
    )

    # 4. Notificação para a API (Refresh Automático)
    refresh_api_task = SimpleHttpOperator(
        task_id='refresh_api_data',
        method='POST',
        http_conn_id='caipora_api_conn',
        endpoint='/admin/refresh-data',
        headers={"X-API-Key": os.getenv("API_PASSWORD")}, # Envia a senha em texto plano do .env
    )   

    # 5. Limpeza do arquivo temporário local
    clean_local_file_task = BashOperator(
        task_id='clean_local_file',
        bash_command=(
            f"FILE_NAME=\"{{{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}}}\"; "
            f"rm -f {STAGING_PATH}/$FILE_NAME"
        )
    )

    # Fluxo de Execução
    dbt_transform_group >> generate_parquet_task >> upload_to_gcs_task >> refresh_api_task >> clean_local_file_task

    if __name__ == "__main__":
        # Permite rodar via: python dag_dbt_transform.py
        dag.test()
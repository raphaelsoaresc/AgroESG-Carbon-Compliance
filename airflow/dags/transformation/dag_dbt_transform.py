import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Imports do Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# Imports Cosmos (dbt)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# Imports para processamento de dados
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely import wkt

# --- CONFIGURAÇÕES DE AMBIENTE (Estilo IBAMA) ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME") # Certifique-se que esta var existe
STAGING_PATH = os.getenv("STAGING_PATH", "/tmp") # Fallback para /tmp se não definido

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

# --- FUNÇÃO PYTHON (Processamento) ---
def generate_compliance_geoparquet(**kwargs):
    ti = kwargs['ti']
    
    print("🛰️ Iniciando extração do BigQuery para GeoParquet...")
    client = bigquery.Client()
    
    # Query na tabela final gerada pelo dbt
    table_id = f"{PROJECT_ID}.agro_esg_marts.fct_compliance_risk"
    query = f"SELECT * FROM `{table_id}`"
    
    df = client.query(query).to_dataframe()
    
    if df.empty:
        raise ValueError(f"A tabela {table_id} está vazia.")

    # Conversão para GeoDataFrame (Essencial para a API ler rápido)
    if 'geometry' in df.columns:
        print("🗺️ Convertendo WKT para Geometria...")
        # Se o BQ retornar WKT (string), converte. Se já vier bytes, ajusta conforme necessário.
        # Geralmente via client python vem como string WKT ou objeto shapely se usar biblioteca certa.
        # Assumindo string WKT padrão do BQ:
        try:
            df['geometry'] = df['geometry'].apply(lambda x: wkt.loads(x) if isinstance(x, str) else x)
            gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        except Exception as e:
            print(f"Erro na conversão de geometria: {e}")
            raise
    else:
        raise ValueError("Coluna 'geometry' não encontrada na tabela fct_compliance_risk.")

    # Limpeza de tipos de data (DuckDB prefere datetime nativo do Python)
    for col in gdf.columns:
        if 'date' in col.lower() or 'timestamp' in col.lower():
            gdf[col] = pd.to_datetime(gdf[col])

    # Definição do nome do arquivo
    file_name = "fct_compliance_latest.parquet"
    output_path = os.path.join(STAGING_PATH, file_name)
    
    print(f"💾 Salvando GeoParquet em: {output_path}")
    gdf.to_parquet(output_path, index=False, compression='snappy')
    
    # Envia o nome do arquivo para o XCom (para a próxima task usar)
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

    # 1. Grupo de Transformação dbt
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
            test_behavior=TestBehavior.AFTER_EACH,
            select=["+fct_compliance_risk"],
        ),
    )

    # 2. Gera o Arquivo Local (Igual ao process_ibama_file)
    generate_parquet_task = PythonOperator(
        task_id='generate_compliance_geoparquet',
        python_callable=generate_compliance_geoparquet,
        provide_context=True
    )

    # 3. Upload para o GCS (Igual ao upload_to_gcs do IBAMA)
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_parquet_to_gcs',
        src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}"),
        dst="api_data/{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}",
        bucket=BUCKET_NAME,
        gcp_conn_id='google_cloud_default',
        mime_type='application/octet-stream',
        
        # --- ADICIONE ESTAS LINHAS ---
        chunk_size=50 * 1024 * 1024,  # Envia em pedaços de 50MB (evita timeout)
        execution_timeout=timedelta(minutes=30), # Dá 30 min para a task rodar antes do Airflow matar
        retries=3, # Tenta 3 vezes se falhar
    )

    # 4. Limpeza Local (Opcional, mas boa prática igual ao archive do IBAMA)
    clean_local_file_task = BashOperator(
        task_id='clean_local_file',
        bash_command=(
            f"FILE_NAME=\"{{{{ ti.xcom_pull(task_ids='generate_compliance_geoparquet', key='parquet_filename') }}}}\"; "
            f"rm -f {STAGING_PATH}/$FILE_NAME"
        )
    )

    # Fluxo: dbt -> Gera Parquet -> Upload GCS -> Limpa Local
    dbt_transform_group >> generate_parquet_task >> upload_to_gcs_task >> clean_local_file_task
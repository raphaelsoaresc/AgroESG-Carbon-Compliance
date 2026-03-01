import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.datasets import Dataset
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior, ExecutionMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# --- DESCOBERTA DINÂMICA ---
BASE_DIR = Path(__file__).resolve().parents[3]

DBT_PROJECT_PATH = BASE_DIR / "agro_credit_transform"
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"
GCP_KEY_PATH = BASE_DIR / "config" / "gcp_credentials.json"
DBT_EXECUTABLE = BASE_DIR / ".devenv" / "state" / "venv" / "bin" / "dbt"

# Configuração do Perfil
profile_config = ProfileConfig(
    profile_name="agro_credit_transform",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": os.getenv("GCP_PROJECT_ID"),
            "dataset": "agro_esg_staging",
            "keyfile": str(GCP_KEY_PATH),
        },
    ),
)

# --- DEFINIÇÃO DA DAG ---
with DAG(
    dag_id="dbt_transformation_medallion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_tasks=3, 
    max_active_runs=1,
    tags=["dbt", "gold", "silver", "esg"],
) as dag:

    dbt_transform_group = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path=str(DBT_PROJECT_PATH),
            manifest_path=str(MANIFEST_PATH), # O MANIFESTO VAI AQUI
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=str(DBT_EXECUTABLE),
            execution_mode=ExecutionMode.LOCAL,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            # install_deps removido pois via MANIFEST o Cosmos não as exige por padrão
        ),
    )

    dbt_transform_group

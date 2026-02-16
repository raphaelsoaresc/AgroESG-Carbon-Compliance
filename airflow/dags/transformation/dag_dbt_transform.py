import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
# Esta classe existe no seu ambiente (confirmado pelo erro anterior)
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# --- DESCOBERTA DINÂMICA ---
BASE_DIR = Path(__file__).resolve().parents[3]

DBT_PROJECT_PATH = BASE_DIR / "agro_credit_transform"
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

# Criação da DAG
dag_dbt_transform = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=str(DBT_EXECUTABLE),
    ),
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_transformation_medallion",
    tags=["dbt", "gold", "silver"],
)
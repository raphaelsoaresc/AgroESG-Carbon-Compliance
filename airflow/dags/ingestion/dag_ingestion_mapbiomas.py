import os
import hashlib
import duckdb
import zipfile
import shutil
import multiprocessing
from datetime import datetime, timedelta
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

# --- CONFIGURAÇÕES ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = os.getenv("BQ_DATASET_ID", "agro_esg_raw")

RAW_PATH = os.getenv("RAW_PATH_MAPBIOMAS", "./data/raw/mapbiomas")
STAGING_PATH = os.getenv("STAGING_PATH", "./data/staging")
ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_MAPBIOMAS", "./data/archive/mapbiomas")

TABLE_SHAPES = "mapbiomas_alertas_shapes"
TABLE_CROSSINGS = "mapbiomas_alertas_car_sigef"

# --- PERFORMANCE ---
CPU_THREADS = max(1, multiprocessing.cpu_count() - 1)
MEMORY_LIMIT = '8GB' 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

def get_file_fingerprint(con, filepath, is_spatial=False):
    try:
        if is_spatial:
            # DuckDB lê o .shp e busca automaticamente o .dbf na mesma pasta
            query = f"SELECT * FROM st_read('{filepath}') LIMIT 0"
        else:
            query = f"SELECT * FROM read_csv_auto('{filepath}', ALL_VARCHAR=TRUE) LIMIT 0"
        
        columns = set([col[0].lower() for col in con.execute(query).description])
        return columns
    except Exception as e:
        print(f"⚠️ Erro fingerprint {filepath}: {e}")
        return set()

def process_files_intelligently(**kwargs):
    ti = kwargs['ti']
    
    if not os.path.exists(RAW_PATH):
        os.makedirs(RAW_PATH, exist_ok=True)
        return []

    # Ignora arquivos ocultos ou temporários
    files = [f for f in os.listdir(RAW_PATH) 
             if not f.startswith('.') and f.lower().endswith(('.zip', '.csv'))]
    
    if not files:
        print("⚠️ Nenhum arquivo ZIP ou CSV encontrado.")
        return []

    detected_types = [] 

    for filename in files:
        full_path = os.path.join(RAW_PATH, filename)
        
        with open(full_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()[:8]

        print(f"🕵️ Analisando: {filename}")
        
        # Pasta temporária para extração completa (CRUCIAL PARA SHAPEFILES)
        temp_extract_dir = os.path.join(RAW_PATH, f"temp_{file_hash}")
        if os.path.exists(temp_extract_dir):
            shutil.rmtree(temp_extract_dir)
        os.makedirs(temp_extract_dir)

        working_path = full_path
        is_zip = filename.lower().endswith('.zip')
        
        # 1. EXTRAÇÃO COMPLETA
        if is_zip:
            try:
                print(f"   📦 Extraindo tudo para {temp_extract_dir}...")
                with zipfile.ZipFile(full_path, 'r') as z:
                    z.extractall(temp_extract_dir)
                
                # Busca recursiva pelo .shp ou .csv dentro da pasta extraída
                # Isso garante que achamos o arquivo mesmo se estiver em subpastas
                found_shps = []
                found_csvs = []
                
                for root, dirs, files_in_dir in os.walk(temp_extract_dir):
                    for f in files_in_dir:
                        if f.lower().endswith('.shp'):
                            found_shps.append(os.path.join(root, f))
                        elif f.lower().endswith('.csv'):
                            found_csvs.append(os.path.join(root, f))
                
                if found_shps:
                    working_path = found_shps[0] # Pega o primeiro shapefile encontrado
                elif found_csvs:
                    working_path = found_csvs[0]
                else:
                    print("   -> ZIP sem .shp ou .csv válidos.")
                    shutil.rmtree(temp_extract_dir)
                    continue
                    
            except Exception as e:
                print(f"   -> Erro ao extrair ZIP: {e}")
                if os.path.exists(temp_extract_dir):
                    shutil.rmtree(temp_extract_dir)
                continue

        # 2. IDENTIFICAÇÃO
        con = duckdb.connect(database=':memory:')
        print(f"🚀 DuckDB: {MEMORY_LIMIT} RAM | {CPU_THREADS} Threads")
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}';")
        con.execute(f"SET threads={CPU_THREADS};")
        con.execute("INSTALL spatial; LOAD spatial;")
        
        is_spatial_file = working_path.lower().endswith('.shp')
        
        # Aqui o DuckDB vai funcionar porque o .dbf e .shx estão na mesma pasta temp
        columns = get_file_fingerprint(con, working_path, is_spatial=is_spatial_file)
        print(f"   -> Colunas: {columns}")

        target_table = None
        
        # Regra 1: CRUZAMENTOS
        if 'codsicar' in columns or 'cod_imovel' in columns or 'cod_sigef' in columns:
            print("   ✅ IDENTIFICADO: MapBiomas Cruzamentos")
            target_table = TABLE_CROSSINGS
            detected_types.append('crossings')

        # Regra 2: ALERTAS
        elif ('alertid' in columns or 'alerta_id' in columns) and ('geom' in columns or 'geometry' in columns):
            print("   ✅ IDENTIFICADO: MapBiomas Alertas")
            target_table = TABLE_SHAPES
            detected_types.append('shapes')
            
        else:
            print("   ❌ NÃO IDENTIFICADO.")
            con.close()
            if os.path.exists(temp_extract_dir):
                shutil.rmtree(temp_extract_dir)
            continue

        # 3. CONVERSÃO
        output_filename = f"{target_table}_{file_hash}.parquet"
        output_path = os.path.join(STAGING_PATH, output_filename)
        
        try:
            if is_spatial_file:
                # Descobre nome da coluna de geometria
                geom_col = 'geom' if 'geom' in columns else 'geometry'
                
                query = f"""
                    COPY (
                        SELECT 
                            * EXCLUDE ({geom_col}), 
                            ST_AsText({geom_col}) as geometry_wkt,
                            '{file_hash}' as file_hash,
                            '{filename}' as source_filename,
                            now() as ingested_at
                        FROM st_read('{working_path}')
                    ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
                """
            else:
                query = f"""
                    COPY (
                        SELECT 
                            *,
                            '{file_hash}' as file_hash,
                            '{filename}' as source_filename,
                            now() as ingested_at
                        FROM read_csv_auto('{working_path}', ALL_VARCHAR=TRUE, NORMALIZE_NAMES=TRUE)
                    ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
                """
            
            print(f"   -> Gerando Parquet...")
            con.execute(query)
            print(f"   -> Sucesso: {output_filename}")

        except Exception as e:
            print(f"   -> Erro na conversão: {e}")
            raise e
        finally:
            con.close()
            # Limpeza da pasta temporária com todos os arquivos extraídos
            if os.path.exists(temp_extract_dir):
                print(f"   🧹 Limpando pasta temporária: {temp_extract_dir}")
                shutil.rmtree(temp_extract_dir)

    return list(set(detected_types))

def decide_which_loads(**kwargs):
    ti = kwargs['ti']
    detected_types = ti.xcom_pull(task_ids='process_files_intelligently')
    
    tasks_to_run = []
    if not detected_types:
        return ['join_tasks']
        
    if 'shapes' in detected_types:
        tasks_to_run.append('load_shapes_bq')
    if 'crossings' in detected_types:
        tasks_to_run.append('load_crossings_bq')
        
    return tasks_to_run

with DAG(
    'ingestion_mapbiomas_intelligent',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['bronze', 'mapbiomas', 'intelligent'],
) as dag:

    process_task = PythonOperator(
        task_id='process_files_intelligently',
        python_callable=process_files_intelligently
    )

    upload_gcs = BashOperator(
        task_id='upload_all_to_gcs',
        bash_command=f"""
            gsutil -m cp {STAGING_PATH}/mapbiomas_*.parquet gs://{BUCKET_NAME}/bronze/mapbiomas/ || true
        """
    )

    branch_task = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_which_loads,
        provide_context=True
    )

    load_shapes = BigQueryInsertJobOperator(
        task_id='load_shapes_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/bronze/mapbiomas/{TABLE_SHAPES}_*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_SHAPES
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        }
    )

    load_crossings = BigQueryInsertJobOperator(
        task_id='load_crossings_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{BUCKET_NAME}/bronze/mapbiomas/{TABLE_CROSSINGS}_*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_CROSSINGS
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True
            }
        }
    )

    join_tasks = EmptyOperator(
        task_id='join_tasks',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    archive_task = BashOperator(
        task_id='archive_and_cleanup',
        bash_command=f"""
            DEST="{ARCHIVE_PATH}/$(date +%Y%m%d)"
            mkdir -p "$DEST"
            mv {RAW_PATH}/* "$DEST/" || true
            rm -f {STAGING_PATH}/mapbiomas_*.parquet
        """
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_transformation_medallion',
        reset_dag_run=True
    )

    process_task >> upload_gcs >> branch_task
    branch_task >> load_shapes >> join_tasks
    branch_task >> load_crossings >> join_tasks
    branch_task >> join_tasks
    join_tasks >> archive_task >> trigger_dbt
import os
import hashlib
import zipfile
from datetime import datetime, timedelta
import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# --- CONFIGURAÇÕES GERAIS
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- FUNÇÃO GERADORA DE DAGS (FACTORY)
def create_dag(uf, bbox=None):
    
    # 1. Configurações do GCP lidas do seu .env
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
    DATASET_ID = os.getenv("BQ_DATASET_ID")
    
    # Nome da tabela no BigQuery será dinâmico por estado (ex: sigef_history_mt)
    TABLE_ID = f"sigef_history_{uf.lower()}"
    
    # 2. Lendo os caminhos base do seu .env
    BASE_RAW_PATH = os.getenv("RAW_PATH_SIGEF", "./data/raw/sigef")
    BASE_STAGING_PATH = os.getenv("STAGING_PATH", "./data/staging")
    BASE_ARCHIVE_PATH = os.getenv("ARCHIVE_PATH_SIGEF", "./data/archive/sigef")

    # 3. Montando os caminhos específicos para a UF atual
    RAW_PATH = os.path.join(BASE_RAW_PATH, uf.upper())
    STAGING_PATH = os.path.join(BASE_STAGING_PATH, uf.upper())
    ARCHIVE_PATH = os.path.join(BASE_ARCHIVE_PATH, uf.upper())

    # 4. Cria as pastas automaticamente no sistema caso não existam
    os.makedirs(RAW_PATH, exist_ok=True)
    os.makedirs(STAGING_PATH, exist_ok=True)
    os.makedirs(ARCHIVE_PATH, exist_ok=True)

    # Função de processamento embutida para herdar as variáveis da UF
    def process_geo_file_with_duckdb(ti):
        # Validação de diretórios
        if not os.path.exists(RAW_PATH):
            raise FileNotFoundError(f"Diretório RAW não encontrado: {RAW_PATH}")

        files =[f for f in os.listdir(RAW_PATH) if f.endswith(('.zip', '.shp'))]
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo do SIGEF encontrado na pasta: {RAW_PATH}")
        
        original_filename = files[0]
        full_path = os.path.join(RAW_PATH, original_filename)
        
        # Gera o Hash do arquivo ORIGINAL para auditoria
        with open(full_path, "rb") as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        
        extracted_files_list =[]
        working_path = full_path
        
        # Tratamento de ZIP (Extração para ler o Shapefile interno)
        if original_filename.endswith('.zip'):
            print(f"Arquivo ZIP detectado: {original_filename}. Extraindo...")
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                extracted_files_list = zip_ref.namelist()
                zip_ref.extractall(RAW_PATH)
                
                # Busca o arquivo .shp dentro do ZIP
                shps =[f for f in extracted_files_list if f.endswith('.shp')]
                if shps:
                    working_path = os.path.join(RAW_PATH, shps[0])
                else:
                    raise ValueError(f"O ZIP {original_filename} não contém um Shapefile (.shp) válido.")

        # Definição do Arquivo de Saída
        output_filename = f"sigef_{uf.lower()}_{file_hash}.parquet"
        output_path = os.path.join(STAGING_PATH, output_filename)
        
        con = duckdb.connect(database=':memory:')
        con.execute("INSTALL spatial; LOAD spatial;")
        
        # Cláusula de filtro espacial opcional
        filter_clause = f"WHERE ST_INTERSECTS(geom, ST_GeomFromText('{bbox}'))" if bbox else ""
        
        # Execução da Query (Convertendo GEOM para texto e adicionando UF)
        query = f"""
            COPY (
                SELECT 
                    * EXCLUDE (geom), 
                    ST_AsText(geom) as geom,
                    '{file_hash}' as file_hash,
                    '{original_filename}' as source_filename,
                    '{uf.upper()}' as uf,
                    now() as ingested_at
                FROM st_read('{working_path}')
                {filter_clause}
            ) TO '{output_path}' (FORMAT 'PARQUET');
        """
        
        print(f"Executando no DuckDB: {query}")
        con.execute(query)
        con.close()

        # Faxina: Remove arquivos extraídos temporários
        if original_filename.endswith('.zip') and extracted_files_list:
            print("🧹 Limpando arquivos extraídos temporários...")
            for f in extracted_files_list:
                file_to_remove = os.path.join(RAW_PATH, f)
                if os.path.exists(file_to_remove):
                    os.remove(file_to_remove)
        
        # Retorno para o Airflow
        ti.xcom_push(key='output_filename', value=output_filename)
        ti.xcom_push(key='original_file', value=original_filename)

    # Definição da DAG
    with DAG(
        f'ingestion_sigef_to_bronze_{uf.lower()}', 
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        is_paused_upon_creation=False,
        tags=['bronze', 'sigef', 'duckdb', uf.lower()],
    ) as dag:

        wait_for_file = FileSensor(
            task_id='wait_for_sigef_file',
            filepath=RAW_PATH,
            fs_conn_id='fs_default',
            poke_interval=30,
            timeout=600,           
            mode='reschedule'          
        )

        process_file = PythonOperator(
            task_id='process_with_duckdb',
            python_callable=process_geo_file_with_duckdb
        )

        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_parquet_to_gcs',
            src=os.path.join(STAGING_PATH, "{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}"),
            dst=f"bronze/sigef/{uf.lower()}/{{{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}}}",
            bucket=BUCKET_NAME,
            gcp_conn_id='google_cloud_default',
            execution_timeout=timedelta(minutes=10),
            mime_type='application/octet-stream'
        )

        load_to_bq = BigQueryInsertJobOperator(
            task_id='load_to_bigquery_bronze',
            configuration={
                "load": {
                    "sourceUris":[f"gs://{BUCKET_NAME}/bronze/sigef/{uf.lower()}/{{{{ ti.xcom_pull(task_ids='process_with_duckdb', key='output_filename') }}}}"],
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET_ID,
                        "tableId": TABLE_ID,
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND",
                    "autodetect": True,
                }
            },
            do_xcom_push=False
        )

        archive_original = BashOperator(
            task_id='archive_original_file',
            bash_command="""
                FILE_NAME="{{ ti.xcom_pull(task_ids='process_with_duckdb', key='original_file') }}";
                FILE_BASE="${FILE_NAME%.*}";
                ARCHIVE_DIR="{{ params.archive_path }}/$(date +%Y%m%d)";
                
                mkdir -p "$ARCHIVE_DIR" && \
                mv -f "{{ params.raw_path }}/${FILE_BASE}".* "$ARCHIVE_DIR/"
            """,
            params={
                'raw_path': RAW_PATH,
                'archive_path': ARCHIVE_PATH
            }
        )


        # Orquestração
        wait_for_file >> process_file >> upload_to_gcs >> load_to_bq >> archive_original
    
    return dag

# --- INSTANCIAÇÃO DAS DAGS ---
# O Airflow vai ler este arquivo e registrar uma DAG separada para cada chamada abaixo.

dag_mt = create_dag('MT') 
dag_am = create_dag('AM')
dag_ro = create_dag('RO')
dag_pa = create_dag('PA')
# dag_pa = create_dag('PA') # Basta descomentar ou adicionar novos estados aqui!
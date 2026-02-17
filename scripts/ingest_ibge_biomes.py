import os
import hashlib
import zipfile
import shutil
from datetime import datetime
import duckdb
from google.cloud import storage
from google.cloud import bigquery


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
DATASET_ID = "agro_esg_raw"
TABLE_ID = "ibge_biomes"

RAW_PATH = "./data/raw/ibge"
STAGING_PATH = "./data/staging"
ARCHIVE_PATH = "./data/archive/ibge"

def process_ibge_biomes():
    # 1. Busca o arquivo na pasta RAW (Igual ao FileSensor/PythonOperator)
    if not os.path.exists(RAW_PATH):
        os.makedirs(RAW_PATH, exist_ok=True)
        raise FileNotFoundError(f"Pasta {RAW_PATH} criada. Coloque o arquivo do IBGE lá.")

    files = [f for f in os.listdir(RAW_PATH) if f.endswith(('.zip', '.shp'))]
    if not files:
        print(f"⚠️ Nenhum arquivo encontrado em {RAW_PATH}. Nada a fazer.")
        return
    
    original_filename = files[0]
    full_path = os.path.join(RAW_PATH, original_filename)
    print(f"📂 Arquivo encontrado: {original_filename}")

    # 2. Gera o Hash (Igual à DAG SIGEF)
    with open(full_path, "rb") as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    extracted_files_list = []
    working_path = full_path

    # 3. Tratamento de ZIP (Igual à DAG SIGEF)
    if original_filename.endswith('.zip'):
        print(f"📦 Arquivo ZIP detectado. Extraindo...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            extracted_files_list = zip_ref.namelist()
            zip_ref.extractall(RAW_PATH)
            
            # Busca o .shp dentro do ZIP
            shps = [f for f in extracted_files_list if f.endswith('.shp')]
            if shps:
                working_path = os.path.join(RAW_PATH, shps[0])
            else:
                raise ValueError(f"O ZIP {original_filename} não contém um Shapefile (.shp) válido.")

    # 4. Definição do Arquivo de Saída (Igual à DAG SIGEF)
    output_filename = f"ibge_biomes_{file_hash}.parquet"
    if not os.path.exists(STAGING_PATH):
        os.makedirs(STAGING_PATH)
    output_path = os.path.join(STAGING_PATH, output_filename)

    # 5. Processamento DuckDB (Padronizado com SIGEF)
    print("🦆 Processando com DuckDB...")
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial; LOAD spatial;")

    # CORREÇÃO:
    # ST_Transform(geom, 'ORIGEM', 'DESTINO')
    # Origem IBGE = 'EPSG:4674' (SIRGAS 2000)
    # Destino BQ  = 'EPSG:4326' (WGS 84)
    
    query = f"""
        COPY (
            SELECT 
                * EXCLUDE (geom), 
                ST_AsText(ST_Transform(geom, 'EPSG:4674', 'EPSG:4326')) as wkt_geometry,
                '{file_hash}' as file_hash,
                '{original_filename}' as source_filename,
                now() as ingested_at
            FROM st_read('{working_path}')
        ) TO '{output_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
    """
    
    con.execute(query)
    con.close()
    print(f"✅ Parquet gerado: {output_path}")

    # 6. Faxina dos arquivos extraídos (Igual à DAG SIGEF)
    if original_filename.endswith('.zip') and extracted_files_list:
        print("🧹 Limpando arquivos extraídos temporários...")
        for f in extracted_files_list:
            file_to_remove = os.path.join(RAW_PATH, f)
            if os.path.exists(file_to_remove) and file_to_remove != full_path:
                os.remove(file_to_remove)

    # 7. Upload para GCS (Simulando LocalFilesystemToGCSOperator)
    gcs_dest = f"bronze/ibge/{output_filename}"
    print(f"☁️ Subindo para GCS: gs://{BUCKET_NAME}/{gcs_dest}")
    
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_dest)
    blob.upload_from_filename(output_path)

    # 8. Carga no BigQuery (Simulando BigQueryInsertJobOperator)
    print(f"🚀 Carregando no BigQuery: {DATASET_ID}.{TABLE_ID}")
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Carga única (substitui)
        autodetect=True,
    )
    
    uri = f"gs://{BUCKET_NAME}/{gcs_dest}"
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"✅ Tabela {TABLE_ID} atualizada com sucesso.")

    # 9. Archive (Simulando BashOperator)
    print("📦 Arquivando arquivo original...")
    today_str = datetime.now().strftime('%Y%m%d')
    archive_dir = os.path.join(ARCHIVE_PATH, today_str)
    os.makedirs(archive_dir, exist_ok=True)
    
    # Move o arquivo original (ZIP ou SHP base)
    shutil.move(full_path, os.path.join(archive_dir, original_filename))
    print(f"✅ Arquivo movido para {archive_dir}")

    # Limpa o parquet local da staging
    if os.path.exists(output_path):
        os.remove(output_path)

if __name__ == "__main__":
    try:
        process_ibge_biomes()
    except Exception as e:
        print(f"❌ Erro: {e}")
import os
import zipfile
import json
import time
from pathlib import Path

import duckdb
import chardet
from dotenv import load_dotenv
from fake_useragent import UserAgent

# Bibliotecas robustas
from curl_cffi import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Google Cloud Stack
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account

load_dotenv()

# --- CONFIGURAÇÕES ---
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID") 
IBAMA_URL = os.getenv("IBAMA_URL_CSV")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME") 
TABLE_NAME = "ibama_embargos_raw"

TEMP_DIR = Path("temp_data")
ZIP_FILE = TEMP_DIR / "ibama.zip"
PARQUET_FILE = TEMP_DIR / "ibama_embargos.parquet"
FALLBACK_FILE = "bronze/ibama_backup.zip" 

# --- AUTENTICAÇÃO ---
service_account_info = os.getenv("GCP_SA_KEY")

if service_account_info:
    info = json.loads(service_account_info)
    credentials = service_account.Credentials.from_service_account_info(info)
    storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
else:
    storage_client = storage.Client()
    bq_client = bigquery.Client()

# --- CONFIGURAÇÃO DE REDE ---
ua = UserAgent()
session = requests.Session(impersonate="chrome")
session.headers.update({
    'User-Agent': ua.random,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,apng,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Referer': 'https://www.gov.br',
})
# --- FUNÇÕES AUXILIARES ---

def get_remote_version():
    """Tenta pegar o ETag do site oficial via HEAD."""
    try:
        response = session.head(IBAMA_URL, timeout=15, verify=False)
        if response.status_code >= 400: return "FORCE_UPDATE"
        
        remote_version = response.headers.get('ETag') or response.headers.get('Last-Modified')
        if not remote_version: return "FORCE_UPDATE"
            
        return remote_version.replace('"', '').strip()
    except Exception:
        return "FORCE_UPDATE"

def get_stored_version(table_ref):
    """Consulta o BigQuery para ver qual versão já temos."""
    query = f"SELECT _source_version FROM `{table_ref}` LIMIT 1"
    try:
        query_job = bq_client.query(query)
        results = list(query_job.result())
        if results: return results[0][0]
        return None
    except Exception:
        return None

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=lambda retry_state: print(f"⏳ Erro na tentativa {retry_state.attempt_number}. Retentando...")
)
def smart_download(url):
    response = session.get(url, stream=True, timeout=60, verify=False)
    response.raise_for_status()
    return response

def upload_to_bucket(local_path, bucket_path):
    """Salva backup no Bucket."""
    try:
        if not BUCKET_NAME: return
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(bucket_path)
        blob.upload_from_filename(local_path)
        print(f"📦 Backup atualizado no Bucket.")
    except Exception as e:
        print(f"⚠️ Falha ao salvar backup: {e}")

def download_from_bucket(bucket_path, local_path):
    """
    Recupera do Bucket E retorna o Hash MD5 do arquivo.
    """
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(bucket_path)
    
    blob.reload() # Garante que temos os metadados frescos
    md5_hash = blob.md5_hash
    
    blob.download_to_filename(local_path)
    print(f"🛟 Fallback: Backup baixado do Bucket (Hash: {md5_hash})")
    return md5_hash

def detect_encoding(file_path):
    print(f"🕵️ Detectando encoding...")
    with open(file_path, 'rb') as f: rawdata = f.read(100000)
    result = chardet.detect(rawdata)
    encoding = result['encoding']
    if not encoding or encoding.upper() == 'ISO-8859-1': return 'windows-1252'
    return encoding

# --- FLUXO PRINCIPAL ---

def extract_load_ibama():
    print(f"--- INGESTÃO RESILIENTE: IBAMA ---")
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    
    # 1. VERIFICAÇÃO INICIAL (SITE)
    print("🔍 Verificando atualizações...")
    remote_version = get_remote_version()
    stored_version = get_stored_version(table_ref)

    print(f"   Versão Site: {remote_version}")
    print(f"   Versão BQ:   {stored_version}")

    # Se o site está ON e a versão é a mesma, para aqui.
    if remote_version != "FORCE_UPDATE" and remote_version == stored_version:
        print("✅ Dados do site já estão atualizados. Parando.")
        return

    print("🔄 Iniciando processo de ingestão...")
    
    if TEMP_DIR.exists(): import shutil; shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(exist_ok=True)

    use_fallback = False
    final_version_to_store = remote_version

    # 2. TENTATIVA DE DOWNLOAD
    try:
        print("⬇️ Tentando download oficial...")
        response = smart_download(IBAMA_URL)
        with open(ZIP_FILE, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Sucesso no download: Atualiza backup
        if BUCKET_NAME:
            upload_to_bucket(str(ZIP_FILE), FALLBACK_FILE)

    except Exception as e:
        print(f"⚠️ Site instável ({e}). Buscando ZIP de segurança no Bucket...")
        
        if not BUCKET_NAME:
            print("❌ Erro: Bucket não configurado e site fora do ar.")
            return
            
        try:
            # Baixa do bucket e PEGA O HASH
            bucket_hash = download_from_bucket(FALLBACK_FILE, ZIP_FILE)
            
            # --- AQUI ESTÁ A TRAVA DE SEGURANÇA ---
            print(f"   Comparando Hash Bucket ({bucket_hash}) com BQ ({stored_version})...")
            
            if stored_version and bucket_hash == stored_version:
                print("🛑 O backup do Bucket é IDÊNTICO ao que já está no BigQuery.")
                print("✅ Nenhuma ação necessária.")
                return # <--- PARA AQUI. NÃO CONTINUA.
            
            use_fallback = True
            final_version_to_store = bucket_hash 
            
        except Exception as e_bucket:
            print(f"🚨 Erro Fatal: Nem o site nem o Bucket estão disponíveis. {e_bucket}")
            return

    # 3. PROCESSAMENTO
    try:
        print("📦 Extraindo arquivo ZIP...")
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(TEMP_DIR)
            csv_path = next(TEMP_DIR.glob("*.csv"))

        print(f"🦆 DuckDB: Convertendo CSV para Parquet...")
        detected_enc = detect_encoding(csv_path)
        con = duckdb.connect()
        
        origem = "backup_bucket" if use_fallback else "ibama_web"
        
        con.execute(f"""
                COPY (
                    SELECT 
                        *, 
                        '{origem}' as _source_mode,
                        '{final_version_to_store}' as _source_version,
                        current_timestamp as _ingested_at
                    FROM read_csv(
                        '{csv_path}', 
                        encoding='{detected_enc}', 
                        sep=';', 
                        all_varchar=True,
                        auto_detect=True,
                        ignore_errors=True,
                        null_padding=True,
                        quote='"'
                    )
                ) TO '{PARQUET_FILE}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
            """)
        con.close()
    except Exception as e_proc:
        print(f"❌ Erro no processamento DuckDB: {e_proc}")
        return

    # 4. CARGA NO BIGQUERY
    print(f"🚀 Enviando para BigQuery (Fallback Mode: {use_fallback})")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        with open(PARQUET_FILE, "rb") as source_file:
            job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Sucesso! Tabela atualizada (Versão: {final_version_to_store}).")
    except Exception as e:
        print(f"❌ Erro final no BigQuery: {e}")
    finally:
        if TEMP_DIR.exists(): import shutil; shutil.rmtree(TEMP_DIR)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    extract_load_ibama()
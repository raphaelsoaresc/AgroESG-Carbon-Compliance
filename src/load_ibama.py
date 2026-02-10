import os
import requests
import zipfile
import duckdb
import chardet  # <--- Biblioteca nova para detecção
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

# Configurações
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID") # Aqui define que vai para agro_esg_raw
IBAMA_URL = os.getenv("IBAMA_URL_CSV")
TABLE_NAME = "ibama_embargos_raw"

# Caminhos Temporários
TEMP_DIR = Path("temp_data")
ZIP_FILE = TEMP_DIR / "ibama.zip"
PARQUET_FILE = TEMP_DIR / "ibama_embargos.parquet"

def detect_encoding(file_path):
    """Lê os primeiros 100KB do arquivo para adivinhar o encoding."""
    print("🕵️ Detectando encoding do arquivo...")
    with open(file_path, 'rb') as f:
        rawdata = f.read(100000) # Lê apenas o começo para ser rápido
    
    result = chardet.detect(rawdata)
    encoding = result['encoding']
    confidence = result['confidence']
    
    print(f"   Encoding detectado: {encoding} (Confiança: {confidence})")
    
    # Fallback de segurança se falhar a detecção
    if not encoding:
        return 'windows-1252'
        
    # Ajuste fino: chardet as vezes retorna 'ISO-8859-1', mas 'windows-1252' é mais seguro para PT-BR
    if encoding.upper() == 'ISO-8859-1':
        return 'windows-1252'
        
    return encoding

def get_remote_version():
    try:
        response = requests.head(IBAMA_URL, timeout=10, verify=False)
        response.raise_for_status()
        remote_version = response.headers.get('ETag') or response.headers.get('Last-Modified')
        if not remote_version: return "FORCE_UPDATE"
        return remote_version.replace('"', '').strip()
    except Exception:
        return "FORCE_UPDATE"

def get_stored_version(client, table_ref):
    query = f"SELECT _source_version FROM `{table_ref}` LIMIT 1"
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        if results: return results[0][0]
        return None
    except (NotFound, Exception):
        return None

def extract_load_ibama():
    print(f"--- INGESTÃO RAW: IBAMA (Auto-Encoding) ---")
    
    client = bigquery.Client(project=PROJECT_ID)
    # Monta o caminho: projeto.agro_esg_raw.ibama_embargos_raw
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

    # 1. VERIFICAÇÃO
    print("🔍 Verificando atualizações...")
    remote_version = get_remote_version()
    stored_version = get_stored_version(client, table_ref)

    if remote_version != "FORCE_UPDATE" and remote_version == stored_version:
        print("✅ Dados já estão atualizados. Nenhuma ação necessária.")
        return

    print("🔄 Nova versão detectada. Iniciando processo...")
    if TEMP_DIR.exists(): import shutil; shutil.rmtree(TEMP_DIR)
    TEMP_DIR.mkdir(exist_ok=True)

    # 2. EXTRACT
    print(f"⬇️ Baixando arquivo...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        with requests.get(IBAMA_URL, headers=headers, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(ZIP_FILE, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        print(f"❌ Erro fatal no download: {e}")
        return

    # 3. UNZIP
    print("📦 Extraindo ZIP...")
    csv_path = None
    try:
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(TEMP_DIR)
            for file_name in zip_ref.namelist():
                if file_name.lower().endswith('.csv'):
                    csv_path = TEMP_DIR / file_name
                    break
    except Exception:
        print("❌ Erro ao extrair ZIP.")
        return

    if not csv_path:
        print("❌ Nenhum CSV encontrado.")
        return

    # 4. DETECT ENCODING (A Mágica acontece aqui)
    detected_enc = detect_encoding(csv_path)

    # 5. TRANSFORM: CSV -> Parquet (DuckDB)
    print(f"🦆 DuckDB: Convertendo usando encoding '{detected_enc}'...")
    con = duckdb.connect()
    try:
        query = f"""
            COPY (
                SELECT 
                    *,
                    '{IBAMA_URL}' as _source_url,
                    '{remote_version}' as _source_version,
                    current_timestamp as _ingested_at
                FROM read_csv(
                    '{csv_path}', 
                    sep=';', 
                    all_varchar=True,
                    auto_detect=True,
                    ignore_errors=True,
                    encoding='{detected_enc}'
                )
            ) TO '{PARQUET_FILE}' (FORMAT 'PARQUET', CODEC 'SNAPPY')
        """
        con.execute(query)
        print(f"Conversão concluída: {PARQUET_FILE}")
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
        return
    finally:
        con.close()

    # 6. LOAD
    print(f"🚀 Enviando para: {table_ref}")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        with open(PARQUET_FILE, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        print(f"✅ Sucesso! Tabela atualizada.")
    except Exception as e:
        print(f"❌ Erro no BigQuery: {e}")
    finally:
        import shutil
        if TEMP_DIR.exists(): shutil.rmtree(TEMP_DIR)

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    extract_load_ibama()
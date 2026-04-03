import os
import gc
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery, storage

# 1. CONFIGURAÇÃO DE AMBIENTE
BASE_DIR = Path(__file__).resolve().parent 
env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
STAGING_PATH = os.getenv("STAGING_PATH", "/tmp")
GCP_KEY_PATH = BASE_DIR / "config" / "gcp_credentials.json"

if not GCP_KEY_PATH.exists():
    GCP_KEY_PATH = Path.cwd() / "config" / "gcp_credentials.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GCP_KEY_PATH)

def process_and_insert_chunk(con, batch, table_name, first_chunk):
    """Sanitização de Encoding e inserção no DuckDB"""
    df = pd.DataFrame(batch)
    
    # Sanitização de Encoding (UTF-8) para evitar quebra no Parquet
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(
            lambda x: str(x).encode('utf-8', 'replace').decode('utf-8') if x is not None else None
        )
    
    if first_chunk:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    else:
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

def export_to_gcs():
    # Inicializa clientes
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    
    # Configuração do DuckDB local
    db_temp_file = os.path.join(STAGING_PATH, "temp_duckdb.db")
    if os.path.exists(db_temp_file): os.remove(db_temp_file)
    
    con = duckdb.connect(db_temp_file)
    con.execute("INSTALL spatial; LOAD spatial;")
    
    # LISTA DE TABELAS PARA EXPORTAÇÃO
    targets = [
        {"id": "fct_compliance_geometries_mart", "file": "fct_compliance_geometries.parquet"},
        {"id": "fct_compliance_risk", "file": "fct_compliance_latest.parquet"}
    ]

    for target in targets:
        table_id = f"{PROJECT_ID}.agro_esg_marts.{target['id']}"
        local_output_path = os.path.join(STAGING_PATH, target['file'])
        
        print(f"\n🦆 Iniciando extração de {table_id}...")
        
        # Limpa a tabela temporária no DuckDB para o próximo alvo
        con.execute("DROP TABLE IF EXISTS tmp_table")

        # 2. EXTRAÇÃO EM LOTES DO BIGQUERY
        # Transformamos a geometria em WKT para transporte seguro via Python
        query = f"""
            SELECT * EXCEPT(geometry), ST_AsText(geometry) as geometry_wkt 
            FROM `{table_id}`
        """
        query_job = bq_client.query(query)
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

        # 3. CONVERSÃO PARA GEOPARQUET (DuckDB Spatial)
        print(f"💾 Convertendo para GeoParquet: {target['file']}")
        
        # Converte o WKT de volta para objeto geométrico nativo do DuckDB
        sql_query = """
            SELECT 
                * EXCLUDE (geometry_wkt),
                CASE WHEN geometry_wkt IS NOT NULL THEN ST_GeomFromText(geometry_wkt) ELSE NULL END AS geometry
            FROM tmp_table
        """
        
        con.execute(f"COPY ({sql_query}) TO '{local_output_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');")

        # 4. UPLOAD PARA GCS COM TIMEOUT ESTENDIDO
        gcs_path = f"api_data/{target['file']}"
        print(f"☁️ Subindo para gs://{BUCKET_NAME}/{gcs_path}")
        
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        # Ajustes para arquivos grandes (como o de 946MB)
        blob.chunk_size = 10 * 1024 * 1024 # 10MB chunks
        
        try:
            blob.upload_from_filename(local_output_path, timeout=1800) # 30 min timeout
            print(f"✨ Upload de {target['file']} concluído!")
        except Exception as e:
            print(f"❌ Erro no upload de {target['file']}: {str(e)}")
            raise
        
        # Remove o arquivo parquet local para liberar espaço em disco antes da próxima tabela
        if os.path.exists(local_output_path): os.remove(local_output_path)

    # 5. LIMPEZA FINAL
    con.close()
    if os.path.exists(db_temp_file): os.remove(db_temp_file)
    
    print("\n🚀 Processo de exportação finalizado com sucesso!")

if __name__ == "__main__":
    export_to_gcs()
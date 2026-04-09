# main.py - VERSÃO FINAL OTIMIZADA PARA GEOPARQUET
import os
import duckdb
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from passlib.context import CryptContext
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict
import numpy as np

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcp_bucket_name: str 
    api_key_hash: str
    
    # Caminhos no GCS (Devem bater com o que o seu pipeline dbt/python gera)
    gcs_risk_path: str = "api_data/fct_compliance_latest.parquet"
    gcs_geom_path: str = "api_data/fct_compliance_geometries.parquet"
    
    # Caminhos Locais Temporários
    local_risk_path: str = "/tmp/fct_compliance_latest.parquet"
    local_geom_path: str = "/tmp/fct_compliance_geometries.parquet"
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()

# --- AUTENTICAÇÃO ---
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(plain_key: str, hashed_key: str):
    return pwd_context.verify(plain_key, hashed_key)

async def get_api_key(header_value: str = Security(api_key_header)):
    if not header_value or not verify_api_key(header_value, settings.api_key_hash):
        raise HTTPException(status_code=403, detail="Acesso negado.")
    return header_value

# --- BANCO DE DADOS (DUCKDB) ---
db_con = None

def load_data():
    """Carrega os arquivos do GCS e registra no DuckDB"""
    global db_con
    tables = [
        {"name": "fct_compliance_latest", "gcs": settings.gcs_risk_path, "local": settings.local_risk_path},
        {"name": "fct_compliance_geometries", "gcs": settings.gcs_geom_path, "local": settings.local_geom_path}
    ]
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(settings.gcp_bucket_name)

        for table in tables:
            print(f"🔄 [GCS] Baixando {table['gcs']}...")
            blob = bucket.blob(table["gcs"])
            
            if not blob.exists():
                print(f"❌ [ERRO] Arquivo não existe no bucket: {table['gcs']}")
                continue

            blob.download_to_filename(table["local"], timeout=600)
            
            # Criando a tabela no DuckDB
            # Nota: O DuckDB Spatial reconhece automaticamente colunas de geometria em GeoParquet
            print(f"✅ [DuckDB] Criando tabela {table['name']}...")
            db_con.execute(f"CREATE OR REPLACE TABLE {table['name']} AS SELECT * FROM read_parquet('{table['local']}')")
            
            # Limpeza do arquivo local para economizar espaço em disco (Cloud Run/Lambda)
            if os.path.exists(table["local"]):
                os.remove(table["local"])
            
            count = db_con.execute(f"SELECT count(*) FROM {table['name']}").fetchone()[0]
            print(f"✨ Tabela {table['name']} pronta com {count} registros.")
        
        return True
    except Exception as e:
        print(f"❌ [ERRO CRÍTICO] Falha ao carregar dados: {str(e)}")
        return False

# --- LIFESPAN (Gerenciamento de Ciclo de Vida) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando Caipora Sentinela...")
    
    # 1. Conecta ao DuckDB em memória
    db_con = duckdb.connect(database=':memory:')
    
    # 2. Configura Extensão Espacial
    try:
        os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
        db_con.execute("SET extension_directory='/tmp/duckdb_extensions';")
        db_con.execute("INSTALL spatial;")
        db_con.execute("LOAD spatial;")
        print("🌍 Extensão SPATIAL carregada com sucesso.")
    except Exception as e:
        print(f"⚠️ Erro ao carregar extensão spatial: {e}")
    
    app.state.db_con = db_con

    # 3. Carrega os dados em uma thread separada para não travar o boot
    asyncio.create_task(asyncio.to_thread(load_data))
    
    yield
    
    if db_con:
        db_con.close()

# --- APP CONFIG ---
app = FastAPI(title="Caipora Sentinela API", version="3.2.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Importação dos Routers (Certifique-se que os arquivos existem)
from routers.compliance import router as compliance_router
# from routers.payments import router as payments_router # Comentado se não for usar agora

app.include_router(compliance_router)
# app.include_router(payments_router)

@app.get("/health", tags=["System"])
async def health():
    tables = []
    ready = False
    if db_con:
        try:
            res = db_con.execute("PRAGMA show_tables").fetchall()
            tables = [t[0] for t in res]
            # Consideramos pronto se as duas tabelas principais estiverem lá
            ready = "fct_compliance_latest" in tables and "fct_compliance_geometries" in tables
        except:
            pass
    return {
        "status": "online" if ready else "loading", 
        "tables_loaded": tables,
        "ready": ready
    }

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    """Força a atualização dos dados a partir do GCS"""
    if load_data():
        return {"message": "Dados atualizados com sucesso!"}
    raise HTTPException(status_code=500, detail="Falha ao atualizar dados.")
# main.py - VERSÃO CORRIGIDA (BLOQUEANTE NO BOOT)

import os
import duckdb
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
    
    gcs_risk_path: str = "api_data/fct_compliance_latest.parquet"
    local_risk_path: str = "/tmp/fct_compliance_latest.parquet"
    
    gcs_geom_path: str = "api_data/fct_compliance_geometries.parquet"
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

# --- BANCO DE DADOS ---
db_con = None

def load_data():
    """Função Síncrona para carregar os dados"""
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
                return False

            blob.download_to_filename(table["local"], timeout=600)
            
            print(f"✅ [DuckDB] Criando tabela {table['name']}...")
            db_con.execute(f"CREATE OR REPLACE TABLE {table['name']} AS SELECT * FROM read_parquet('{table['local']}')")
            
            count = db_con.execute(f"SELECT count(*) FROM {table['name']}").fetchone()[0]
            print(f"✨ Tabela {table['name']} pronta com {count} registros.")
        
        return True
    except Exception as e:
        print(f"❌ [ERRO CRÍTICO] Falha ao carregar dados: {str(e)}")
        return False

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando Caipora Sentinela...")
    
    # 1. Conecta ao DuckDB
    db_con = duckdb.connect(database=':memory:') 
    
    # 2. Configura Espacial
    os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
    db_con.execute("SET extension_directory='/tmp/duckdb_extensions';")
    db_con.execute("INSTALL spatial; LOAD spatial;")
    
    # 3. CARGA OBRIGATÓRIA (BLOQUEANTE)
    # A API só vai terminar de subir quando os dados estiverem no DuckDB
    success = load_data()
    if not success:
        print("⚠️ AVISO: A carga inicial falhou. A API pode retornar erros 500.")
    
    app.state.db_con = db_con
    
    yield
    if db_con:
        db_con.close()

# --- APP ---
app = FastAPI(title="Caipora Sentinela API", version="3.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

from routers.compliance import router as compliance_router
from routers.payments import router as payments_router
app.include_router(compliance_router)
app.include_router(payments_router)

@app.get("/health", tags=["System"])
async def health():
    tables = []
    if db_con:
        try:
            res = db_con.execute("PRAGMA show_tables").fetchall()
            tables = [t[0] for t in res]
        except:
            pass
    return {
        "status": "online", 
        "tables_loaded": tables,
        "ready": len(tables) >= 2
    }

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    if os.path.exists(settings.local_risk_path): os.remove(settings.local_risk_path)
    if os.path.exists(settings.local_geom_path): os.remove(settings.local_geom_path)
    if load_data():
        return {"message": "Dados atualizados com sucesso!"}
    raise HTTPException(status_code=500, detail="Falha ao atualizar dados.")
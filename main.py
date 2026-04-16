import os
import duckdb
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
# google.cloud import storage removido pois o DuckDB httpfs substitui
from passlib.context import CryptContext
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    gcp_bucket_name: str 
    api_key_hash: str
    gcs_risk_path: str = "api_data/fct_compliance_latest.parquet"
    gcs_geom_path: str = "api_data/fct_compliance_geometries.parquet"
    gcp_hmac_access_key: str
    gcp_hmac_secret_key: str
    gcp_region: str = "us-central1"
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
data_ready = False # Flag para o Health Check

def init_db_connection():
    con = duckdb.connect(database=':memory:')
    os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
    con.execute("SET extension_directory='/tmp/duckdb_extensions';")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    con.execute(f"SET s3_region='{settings.gcp_region}';")
    con.execute(f"SET s3_access_key_id='{settings.gcp_hmac_access_key}';")
    con.execute(f"SET s3_secret_access_key='{settings.gcp_hmac_secret_key}';")
    con.execute("SET s3_endpoint='storage.googleapis.com';")
    
    con.execute("PRAGMA enable_object_cache;")
    con.execute("SET threads=4;") 
    con.execute("SET http_keep_alive=true;")
    return con

async def load_data_task():
    """Mapeia os arquivos de 1GB+ em segundo plano"""
    global db_con, data_ready
    try:
        uri_risk = f"s3://{settings.gcp_bucket_name}/{settings.gcs_risk_path}"
        uri_geom = f"s3://{settings.gcp_bucket_name}/{settings.gcs_geom_path}"
        
        print(f"⏳ [DuckDB] Mapeando arquivos pesados...")
        # O DuckDB vai ler os metadados dos arquivos de 1GB aqui
        db_con.execute(f"CREATE OR REPLACE TABLE fct_compliance_latest AS SELECT * FROM read_parquet('{uri_risk}')")
        db_con.execute(f"CREATE OR REPLACE TABLE fct_compliance_geometries AS SELECT * FROM read_parquet('{uri_geom}')")
        
        data_ready = True
        print("✅ [DuckDB] Tabelas prontas para consulta.")
    except Exception as e:
        print(f"❌ [DuckDB] Erro ao carregar: {e}")

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando Caipora Sentinela...")
    
    # 1. Conecta e instala extensões (Rápido)
    db_con = init_db_connection()
    app.state.db_con = db_con

    # 2. Dispara o mapeamento dos arquivos de 1GB em segundo plano
    # Isso permite que a API responda "OK" para o Google Cloud imediatamente
    asyncio.create_task(load_data_task())
    
    yield
    if db_con:
        db_con.close()

# --- APP CONFIG ---
app = FastAPI(title="Caipora Sentinela API", version="3.5.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

from routers.compliance import router as compliance_router
app.include_router(compliance_router)

@app.get("/health", tags=["System"])
async def health():
    return {
        "status": "online" if data_ready else "loading", 
        "database": "DuckDB + GCS Streaming",
        "ready": data_ready
    }

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    # Simplesmente dispara a task novamente
    asyncio.create_task(load_data_task())
    return {"message": "Atualização de dados iniciada em segundo plano."}
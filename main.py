import os
import asyncio
import duckdb
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from passlib.context import CryptContext
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcp_bucket_name: str 
    api_key_hash: str
    
    # Tabela 1: Dados e Relatórios (Risk)
    gcs_risk_path: str = "api_data/fct_compliance_latest.parquet"
    local_risk_path: str = "/tmp/compliance_risk.parquet"
    
    # Tabela 2: Mapas e Geometrias (Geometries)
    gcs_geom_path: str = "api_data/fct_compliance_geometries.parquet"
    local_geom_path: str = "/tmp/compliance_geometries.parquet"
    
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
    global db_con
    tables = [
        {"name": "compliance_data", "gcs": settings.gcs_risk_path, "local": settings.local_risk_path},
        {"name": "map_data", "gcs": settings.gcs_geom_path, "local": settings.local_geom_path}
    ]
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(settings.gcp_bucket_name)

        for table in tables:
            print(f"🔄 [GCS] Baixando {table['gcs']}...")
            blob = bucket.blob(table["gcs"])
            blob.download_to_filename(table["local"], timeout=600)
            
            print(f"✅ [DuckDB] Carregando {table['name']}...")
            # Agora que o dado é UTF-8, o read_parquet padrão funciona perfeitamente
            db_con.execute(f"CREATE OR REPLACE TABLE {table['name']} AS SELECT * FROM read_parquet('{table['local']}')")
            
            count = db_con.execute(f"SELECT count(*) FROM {table['name']}").fetchone()[0]
            print(f"✨ Tabela {table['name']} pronta: {count} registros.")
        
        return True
    except Exception as e:
        print(f"❌ [ERRO CRÍTICO] Falha no carregamento: {e}")
        return False

async def background_load_data():
    """Aguarda a API subir e inicia a carga pesada"""
    await asyncio.sleep(2)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, load_data)

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando API Caipora Sentinela...")
    
    db_con = duckdb.connect(database=':memory:') 
    os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
    db_con.execute("SET extension_directory='/tmp/duckdb_extensions';")
    db_con.execute("INSTALL spatial; LOAD spatial;")
    
    app.state.db_con = db_con
    asyncio.create_task(background_load_data())
    
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
        "tables_loaded": tables
    }

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    if os.path.exists(settings.local_risk_path): os.remove(settings.local_risk_path)
    if os.path.exists(settings.local_geom_path): os.remove(settings.local_geom_path)
    if load_data():
        return {"message": "Dados atualizados com sucesso!"}
    raise HTTPException(status_code=500, detail="Falha ao atualizar dados.")
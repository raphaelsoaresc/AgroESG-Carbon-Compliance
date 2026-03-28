import os
import duckdb
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from passlib.context import CryptContext
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field

# Importando os roteadores refatorados
from routers.compliance import router as compliance_router
from routers.payments import router as payments_router

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcp_bucket_name: str 
    api_key_hash: str
    gcs_parquet_path: str = "api_data/fct_compliance_latest.parquet"
    local_parquet_path: str = "/tmp/compliance_data.parquet"
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()

# --- AUTENTICAÇÃO ADMIN (API KEY) ---
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(plain_key: str, hashed_key: str):
    return pwd_context.verify(plain_key, hashed_key)

async def get_api_key(header_value: str = Security(api_key_header)):
    if not header_value or not verify_api_key(header_value, settings.api_key_hash):
        raise HTTPException(
            status_code=403, 
            detail="Acesso negado: Token de Admin inválido ou ausente."
        )
    return header_value

# --- BANCO DE DADOS ---
db_con = None

def load_data():
    global db_con
    local_file = settings.local_parquet_path
    
    if os.path.exists(local_file):
        print(f"✅ Arquivo local encontrado em {local_file}. Pulando download...")
    else:
        print(f"🔄 Baixando dados do GCS ({settings.gcs_parquet_path})...")
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(settings.gcp_bucket_name)
            blob = bucket.blob(settings.gcs_parquet_path)
            blob.download_to_filename(local_file)
            print("✅ Download concluído.")
        except Exception as e:
            print(f"❌ Erro ao baixar do GCS: {e}")
            return False

    try:
        db_con.execute(f"CREATE OR REPLACE TABLE compliance_data AS SELECT * FROM read_parquet('{local_file}')")
        count = db_con.execute("SELECT count(*) FROM compliance_data").fetchone()[0]
        print(f"✅ DuckDB carregado: {count} registros.")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar no DuckDB: {e}")
        return False

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando API Caipora Sentinela...")
    
    db_con = duckdb.connect(database=':memory:') 
    os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
    db_con.execute("SET extension_directory='/tmp/duckdb_extensions';")
    db_con.execute("INSTALL spatial; LOAD spatial;")

    success = load_data()
    if not success:
        print("❌ FALHA CRÍTICA: Não foi possível carregar os dados.")
    
    # IMPORTANTE: Injeta a conexão no app.state para os routers poderem usar!
    app.state.db_con = db_con
    
    yield
    if db_con:
        db_con.close()

# --- INICIALIZAÇÃO DO APP ---
app = FastAPI(
    title="Caipora Sentinela API", 
    version="3.1.0", 
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Registrando as rotas refatoradas
app.include_router(compliance_router)
app.include_router(payments_router)

# --- ROTAS DE SISTEMA / ADMIN ---
class HealthResponse(BaseModel):
    status: str = Field(..., example="online")
    engine: str = Field(..., example="DuckDB + Spatial")
    version: str = Field(..., example="3.1.0")

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    return {"status": "online", "engine": "DuckDB + Spatial", "version": "3.1.0"}

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    """Força a API a baixar o GeoParquet mais recente do GCS (Requer API Key)."""
    success = load_data()
    if success:
        return {"message": "Dados atualizados com sucesso!"}
    raise HTTPException(status_code=500, detail="Falha ao atualizar dados.")
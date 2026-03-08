import time
import io
import os
import duckdb
import pandas as pd
import numpy as np  # <-- Adicionado para suportar np.nan
from typing import List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from pydantic_settings import BaseSettings

# Importações dos seus schemas locais
from schemas import (ComplianceResponse, CoordinatePoint, 
                    BatchCoordinateRequest, CSVUploadResponse)

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcs_bucket_name: str = os.getenv("GCS_BUCKET_NAME", "nome-do-seu-bucket-aqui")
    gcs_parquet_path: str = "api_data/fct_compliance_latest.parquet"
    local_parquet_path: str = "compliance_data.parquet"
    
    model_config = {
        "env_file": ".env",
        "extra": "ignore" 
    }

settings = Settings()
db_con = None

# --- LIFESPAN (STARTUP & SHUTDOWN) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando API - Preparando Motor Geoespacial...")
    
    local_file = settings.local_parquet_path
    
    print(f"☁️  Baixando gs://{settings.gcs_bucket_name}/{settings.gcs_parquet_path} ...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(settings.gcs_bucket_name)
        blob = bucket.blob(settings.gcs_parquet_path)
        blob.download_to_filename(local_file)
        print(f"✅ Download concluído: {local_file}")
    except Exception as e:
        print(f"⚠️  Erro ao baixar do GCS: {e}")
        if not os.path.exists(local_file):
            print("❌ Arquivo local não encontrado. A API pode falhar nas consultas.")

    db_con = duckdb.connect(database=':memory:') 
    print("🌍 Carregando extensão SPATIAL do DuckDB...")
    db_con.execute("INSTALL spatial; LOAD spatial;")
    
    if os.path.exists(local_file):
        print(f"📂 Montando tabela 'compliance_data' a partir de {local_file}...")
        db_con.execute(f"""
            CREATE OR REPLACE VIEW compliance_data AS 
            SELECT * FROM '{local_file}'
        """)
        
        try:
            count = db_con.execute("SELECT COUNT(*) FROM compliance_data").fetchone()[0]
            print(f"✅ Motor pronto! {count:,} registros carregados.")
        except Exception as e:
            print(f"⚠️ Erro ao ler o arquivo Parquet: {e}")
    
    yield
    print("🛑 Encerrando conexão DuckDB...")
    db_con.close()

# --- APP SETUP ---
app = FastAPI(
    title="Caipora Sentinela API",
    description="Monitoramento de Compliance Socioambiental e Risco de Crédito.",
    version="3.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- FUNÇÕES AUXILIARES ---
def map_row_to_response(row, reference_id=None):
    status = row.get("final_eligibility_status", "UNKNOWN")
    prop_alias = row.get("property_alias")
    prop_name = prop_alias 
    embargo_date = row.get("embargo_date") 
    embargo_area = row.get("embargo_area_ha")
    is_protected = bool(row.get("is_protected_area_overlap"))

    return {
        "reference_id": reference_id if reference_id else row.get("reference_id"),
        "property_id": str(row.get("property_id", "")),
        "property_name": prop_name if prop_name else "Não Informado",
        "property_alias": prop_alias if prop_alias else "Sem Alias",
        "total_area_ha": float(row.get("property_area_ha", 0) or 0),
        "verdict": status,
        "environmental_score": {
            "biome": row.get("biome_name") or "Desconhecido",
            "legal_reserve_required_pct": 0.0, 
            "has_app_area": False, 
            "critical_app_violation": "CRITICAL APP VIOLATION" in str(status),
            "is_eudr_compliant": bool(row.get("is_eudr_compliant", False))
        },
        "social_score": {
            "indigenous_land_overlap": is_protected,
            "quilombola_land_overlap": False
        },
        "risk_analysis": {
            "oldest_embargo_date": str(embargo_date) if embargo_date else None,
            "total_embargoed_area_ha": float(embargo_area or 0),
            "adjacency_contamination_risk": "CONTAMINATION" in str(status)
        }
    }

def _execute_batch_duckdb(points: List[CoordinatePoint]) -> List[dict]:
    if not points:
        return []

    input_data = [
        {"lat": float(p.lat), "lon": float(p.lon), "ref_id": str(p.reference_id)} 
        for p in points
    ]
    input_df = pd.DataFrame(input_data)
    
    view_name = f"batch_input_{int(time.time()*1000)}"
    db_con.register(view_name, input_df)
    
    query = f"""
        SELECT 
            target.*, 
            src.ref_id as input_reference_id
        FROM compliance_data AS target
        JOIN {view_name} AS src 
          ON ST_Contains(target.geometry, ST_Point(src.lon, src.lat))
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY src.ref_id 
            ORDER BY target.property_area_ha DESC
        ) = 1
    """
    
    try:
        results_df = db_con.execute(query).df()
        
        # --- ALTERAÇÃO 1: Conversão exata solicitada ---
        # Substitui NaNs e NaTs por None antes de converter para dicionário
        results_df = results_df.replace({np.nan: None, pd.NaT: None})
        # -----------------------------------------------
        
        response = []
        for _, row in results_df.iterrows():
            row_dict = row.to_dict()
            response.append(map_row_to_response(row_dict, reference_id=row_dict.get('input_reference_id')))
            
        return response
        
    except Exception as e:
        print(f"Erro na query DuckDB: {e}")
        raise HTTPException(status_code=500, detail=f"Erro no processamento espacial: {str(e)}")
    finally:
        db_con.execute(f"DROP VIEW IF EXISTS {view_name}")

# --- ENDPOINTS ---

@app.get("/health", tags=["Infrastructure"])
async def health_check():
    try:
        db_con.execute("SELECT 1")
        return {"status": "healthy", "engine": "DuckDB", "timestamp": time.time()}
    except:
        raise HTTPException(status_code=503, detail="Database not ready")

@app.get("/compliance/point", response_model=ComplianceResponse, tags=["Compliance"])
async def get_compliance_by_point(lat: float, lon: float):
    query = """
        SELECT * 
        FROM compliance_data 
        WHERE ST_Contains(geometry, ST_Point(?, ?)) 
        LIMIT 1
    """
    try:
        result_df = db_con.execute(query, [lon, lat]).df()
        
        # --- ALTERAÇÃO 2: Conversão exata solicitada ---
        result_df = result_df.replace({np.nan: None, pd.NaT: None})
        # -----------------------------------------------
        
        if result_df.empty:
            raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
        
        row = result_df.iloc[0].to_dict()
        return map_row_to_response(row)
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f"Erro interno DuckDB: {str(e)}")

@app.post("/compliance/batch/points", response_model=List[ComplianceResponse], tags=["Compliance"])
async def get_batch_compliance_by_points(request: BatchCoordinateRequest):
    return _execute_batch_duckdb(request.points)

@app.post("/compliance/batch/csv", response_model=CSVUploadResponse, tags=["Compliance"])
async def process_csv_compliance(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")
    
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig', sep=None, engine='python')
    except Exception:
        df = pd.read_csv(io.BytesIO(content), encoding='latin1', sep=None, engine='python')

    df_limited = df.head(settings.max_csv_rows)
    points = []

    if 'coords' in df_limited.columns:
        for idx, row in df_limited.iterrows():
            try:
                c_str = str(row['coords']).replace('(', '').replace(')', '').replace('[', '').replace(']', '').replace(' ', '')
                parts = c_str.replace(';', ',').split(',')
                if len(parts) >= 2:
                    points.append(CoordinatePoint(
                        lat=float(parts[0]), 
                        lon=float(parts[1]), 
                        reference_id=str(row.get('property_alias', f"line_{idx}"))
                    ))
            except: continue

    if not points:
        lat_col = next((c for c in df_limited.columns if 'lat' in c.lower()), None)
        lon_col = next((c for c in df_limited.columns if 'log' in c.lower() or 'lon' in c.lower()), None)
        
        if lat_col and lon_col:
            for idx, row in df_limited.iterrows():
                try:
                    lat_val = str(row[lat_col]).replace(',', '.')
                    lon_val = str(row[lon_col]).replace(',', '.')
                    
                    points.append(CoordinatePoint(
                        lat=float(lat_val),
                        lon=float(lon_val),
                        reference_id=str(row.get('property_alias', idx))
                    ))
                except: continue

    if not points:
        raise HTTPException(status_code=400, detail="Nenhuma coordenada válida encontrada (verifique colunas 'coords' ou 'lat'/'lon').")

    # O _execute_batch_duckdb já faz a limpeza de NaNs/NaTs internamente
    results = _execute_batch_duckdb(points)
    
    return {"total_processed": len(results), "results": results}
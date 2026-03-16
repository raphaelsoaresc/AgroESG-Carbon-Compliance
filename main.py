import time
import io
import os
import json
import duckdb
import pandas as pd
import numpy as np
from typing import List, Optional, Union, Dict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, Body
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from pydantic import BaseModel, Field
from passlib.context import CryptContext
from fastapi.security import APIKeyHeader
from fastapi import Security, Depends, HTTPException
from pydantic_settings import BaseSettings, SettingsConfigDict

# Importações dos seus schemas locais
from schemas import (
    ComplianceResponse, CoordinatePoint, 
    BatchCoordinateRequest, CSVUploadResponse,
    EnvironmentalScore, DeforestationMetrics, SocialScore, RiskAnalysis, FinancialLiabilities
)

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcp_bucket_name: str 
    api_key_hash: str
    gcs_parquet_path: str = "api_data/fct_compliance_latest.parquet"
    local_parquet_path: str = "/tmp/compliance_data.parquet"
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(plain_key: str, hashed_key: str):
    return pwd_context.verify(plain_key, hashed_key)

async def get_api_key(header_value: str = Security(api_key_header)):
    if not header_value or not verify_api_key(header_value, settings.api_key_hash):
        raise HTTPException(
            status_code=403, 
            detail="Acesso negado: Token inválido ou ausente."
        )
    return header_value

db_con = duckdb.connect(database=':memory:')

# --- FUNÇÃO DE CARGA REUTILIZÁVEL ---
def load_data():
    global db_con
    local_file = settings.local_parquet_path
    
    print("🔄 Atualizando dados do GCS...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(settings.gcp_bucket_name)
        blob = bucket.blob(settings.gcs_parquet_path)
        
        # Força o download sobrescrevendo o arquivo local
        blob.download_to_filename(local_file)
        print(f"✅ Download da versão mais recente concluído.")
        
        # Recarrega no DuckDB
        db_con.execute(f"CREATE OR REPLACE TABLE compliance_data AS SELECT * FROM read_parquet('{local_file}')")
        print("✅ Tabela 'compliance_data' atualizada na memória.")
        return True
    except Exception as e:
        print(f"❌ Erro ao atualizar: {e}")
        return False

# --- SCHEMAS DE ENTRADA E SAÍDA ---

class HealthResponse(BaseModel):
    status: str = Field(..., example="online")
    engine: str = Field(..., example="DuckDB + Spatial")
    version: str = Field(..., example="3.1.0")

class PolygonRequest(BaseModel):
    wkt: Optional[str] = Field(
        None, 
        description="Geometria em formato Well-Known Text (WKT)",
        examples=["POLYGON((-48.5 -22.5, -48.4 -22.5, -48.4 -22.6, -48.5 -22.6, -48.5 -22.5))"]
    )
    geojson: Optional[Dict] = Field(
        None, 
        description="Geometria em formato GeoJSON",
        examples=[{"type": "Point", "coordinates": [-48.5, -22.5]}]
    )
    reference_id: Optional[str] = Field(None, example="REF-123")

# --- LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_con
    print("🚀 Iniciando API Caipora Sentinela...")
    
    # 1. Configura DuckDB e Extensões
    db_con = duckdb.connect(database=':memory:') 
    os.makedirs('/tmp/duckdb_extensions', exist_ok=True)
    db_con.execute("SET extension_directory='/tmp/duckdb_extensions';")
    db_con.execute("INSTALL spatial; LOAD spatial;")

    # 2. Carga inicial de dados
    load_data()
    
    yield
    if db_con:
        db_con.close()

app = FastAPI(
    title="Caipora Sentinela API", 
    version="3.1.0", 
    lifespan=lifespan,
    dependencies=[Depends(get_api_key)]
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- FUNÇÕES AUXILIARES ---

def map_row_to_response(row: dict, reference_id: str = None) -> ComplianceResponse:
    def clean_num(val, default=0.0):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return default
        return float(val)

    geometry_json = None
    raw_geometry = row.get("geometry")
    if raw_geometry:
        try:
            geometry_json = json.loads(raw_geometry) if isinstance(raw_geometry, str) else raw_geometry
        except Exception:
            geometry_json = None

    has_slave_labor = row.get("slave_labor_offender") is not None and str(row.get("slave_labor_offender")).strip() != ""
    has_adjacency_risk = row.get("adjacency_details") is not None and str(row.get("adjacency_details")) != "None"

    return ComplianceResponse(
        reference_id=reference_id or str(row.get("reference_id", "")),
        property_id=str(row.get("property_id", "")),
        property_name=str(row.get("property_alias", "Não Informado")),
        property_alias=str(row.get("property_alias", "Sem Alias")),
        total_area_ha=clean_num(row.get("property_area_ha")),
        verdict=str(row.get("final_eligibility_status", "UNKNOWN")),
        city=str(row.get("city", "Não Informada")),
        uf=str(row.get("uf_origem", "Não Informada")),
        car_status=str(row.get("car_status", "ATIVO")),
        max_slope_degrees=clean_num(row.get("max_slope_degrees")),
        geometry=geometry_json,
        financial_liabilities=FinancialLiabilities(
            estimated_total_brl=clean_num(row.get("estimated_financial_liability_brl")),
            deforestation_liability=clean_num(row.get("liability_deforestation_brl")),
            social_liability=clean_num(row.get("liability_social_brl"))
        ),
        satellite_image_date=row.get("satellite_image_date"),
        protected_area_overlap_ha=clean_num(row.get("protected_area_overlap_ha")),
        environmental_score=EnvironmentalScore(
            biome=str(row.get("biome_name", "Desconhecido")),
            legal_reserve_required_pct=0.0,
            has_app_area=clean_num(row.get("app_ndvi_mean")) > 0,
            critical_app_violation="SATELLITE" in str(row.get("internal_risks_found", "")),
            is_eudr_compliant="EUDR" not in str(row.get("final_eligibility_status", "")),
            general_ndvi_mean=clean_num(row.get("general_ndvi_mean")),
            app_ndvi_mean=clean_num(row.get("app_ndvi_mean")),
            rl_deficit_ha=clean_num(row.get("rl_deficit_ha")),
            rl_balance_ha=clean_num(row.get("rl_balance_ha"))
        ),
        deforestation_metrics=DeforestationMetrics(
            mapbiomas_deforested_ha=clean_num(row.get("mapbiomas_deforested_ha")),
            eudr_deforested_ha=clean_num(row.get("eudr_deforested_ha")),
            mapbiomas_date=row.get("mapbiomas_date"),
            mapbiomas_alert_id=str(row.get("mapbiomas_alert_id")) if row.get("mapbiomas_alert_id") else None
        ),
        social_score=SocialScore(
            indigenous_land_overlap=bool(row.get("is_protected_area_overlap", False)),
            quilombola_land_overlap=bool(row.get("is_protected_area_overlap", False)),
            slave_labor_offender=has_slave_labor
        ),
        risk_analysis=RiskAnalysis(
            oldest_embargo_date=row.get("embargo_date"),
            total_embargoed_area_ha=clean_num(row.get("embargo_area_ha")),
            adjacency_contamination_risk=has_adjacency_risk,
            technical_evidence=str(row.get("technical_evidence", "")),
            internal_risks_found=str(row.get("internal_risks_found", "")),
            adjacency_details=str(row.get("adjacency_details", ""))
        )
    )

# --- ENDPOINTS ---

@app.post("/admin/refresh-data", tags=["System"])
async def refresh_data(api_key: str = Depends(get_api_key)):
    """Força a API a baixar o GeoParquet mais recente do GCS e recarregar a tabela em memória."""
    success = load_data()
    if success:
        return {"message": "Dados atualizados com sucesso!"}
    raise HTTPException(status_code=500, detail="Falha ao atualizar dados.")

@app.get("/compliance/car/{car_id}", response_model=ComplianceResponse, tags=["Compliance"])
async def get_by_car(car_id: str):
    query = "SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE property_id = ?"
    result_df = db_con.execute(query, [car_id]).df()
    if result_df.empty:
        raise HTTPException(status_code=404, detail="Código do CAR não encontrado.")
    row = result_df.replace({np.nan: None}).to_dict(orient="records")[0]
    return map_row_to_response(row)

@app.get("/compliance/point", response_model=ComplianceResponse, tags=["Compliance"])
async def get_by_point(lat: float, lon: float):
    query = "SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE ST_Contains(geometry, ST_Point(?, ?)) LIMIT 1"
    result_df = db_con.execute(query, [lon, lat]).df()
    if result_df.empty:
        raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
    row = result_df.replace({np.nan: None}).iloc[0].to_dict()
    return map_row_to_response(row)

@app.post("/compliance/polygon", response_model=List[ComplianceResponse], tags=["Compliance"])
async def get_by_polygon(request: PolygonRequest):
    try:
        if request.wkt and request.wkt.strip():
            geom_query = "ST_GeomFromText(?)"
            param = request.wkt
        elif request.geojson:
            geom_query = "ST_GeomFromGeoJSON(?)"
            param = json.dumps(request.geojson)
        else:
            raise HTTPException(status_code=400, detail="Forneça um WKT ou GeoJSON válido.")

        query = f"SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE ST_Intersects(geometry, {geom_query})"
        result_df = db_con.execute(query, [param]).df()
        return [map_row_to_response(row.to_dict()) for _, row in result_df.replace({np.nan: None}).iterrows()]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro na geometria: {str(e)}")

@app.post("/compliance/batch/csv", response_model=CSVUploadResponse, tags=["Compliance"])
async def process_csv_compliance(file: UploadFile = File(...)):
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content)).head(settings.max_csv_rows)
    results = []
    car_col = next((c for c in df.columns if 'car' in c.lower() or 'property_id' in c.lower()), None)
    lat_col = next((c for c in df.columns if 'lat' in c.lower()), None)
    lon_col = next((c for c in df.columns if 'lon' in c.lower() or 'log' in c.lower()), None)
    wkt_col = next((c for c in df.columns if 'wkt' in c.lower() or 'geometry' in c.lower()), None)

    for idx, row in df.iterrows():
        try:
            res_df = pd.DataFrame()
            ref_id = str(row.get('reference_id', idx))
            if car_col and pd.notnull(row[car_col]):
                res_df = db_con.execute("SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE property_id = ? OR property_alias = ? LIMIT 1", [str(row[car_col]), str(row[car_col])]).df()
            elif wkt_col and pd.notnull(row[wkt_col]):
                res_df = db_con.execute("SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE ST_Intersects(geometry, ST_GeomFromText(?)) LIMIT 1", [str(row[wkt_col])]).df()
            elif lat_col and lon_col and pd.notnull(row[lat_col]):
                res_df = db_con.execute("SELECT * EXCLUDE (geometry), ST_AsGeoJSON(geometry) AS geometry FROM compliance_data WHERE ST_Contains(geometry, ST_Point(?, ?)) LIMIT 1", [float(row[lon_col]), float(row[lat_col])]).df()

            if not res_df.empty:
                row_dict = res_df.replace({np.nan: None}).iloc[0].to_dict()
                results.append(map_row_to_response(row_dict, reference_id=ref_id))
        except Exception:
            continue
    return {"total_processed": len(results), "results": results}

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    return {"status": "online", "engine": "DuckDB + Spatial", "version": "3.1.0"}
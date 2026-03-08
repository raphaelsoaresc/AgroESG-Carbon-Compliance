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
from pydantic_settings import BaseSettings

# Importações dos seus schemas locais
from schemas import (
    ComplianceResponse, CoordinatePoint, 
    BatchCoordinateRequest, CSVUploadResponse,
    EnvironmentalScore, DeforestationMetrics, SocialScore, RiskAnalysis
)

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    gcs_bucket_name: str 
    gcs_parquet_path: str = "api_data/fct_compliance_latest.parquet"
    local_parquet_path: str = "compliance_data.parquet"
    
    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()
db_con = None

# --- NOVOS SCHEMAS DE ENTRADA E SAÍDA ---

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
    
    local_file = settings.local_parquet_path
    if not os.path.exists(local_file):
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(settings.gcs_bucket_name)
            blob = bucket.blob(settings.gcs_parquet_path)
            blob.download_to_filename(local_file)
            print(f"✅ Download concluído: {local_file}")
        except Exception as e:
            print(f"⚠️ Erro ao baixar do GCS: {e}")

    db_con = duckdb.connect(database=':memory:') 
    db_con.execute("INSTALL spatial; LOAD spatial;")
    
    if os.path.exists(local_file):
        db_con.execute(f"CREATE OR REPLACE VIEW compliance_data AS SELECT * FROM '{local_file}'")
        print("✅ Tabela 'compliance_data' montada.")
    
    yield
    db_con.close()

app = FastAPI(title="Caipora Sentinela API", version="3.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- FUNÇÕES AUXILIARES ---

def map_row_to_response(row: dict, reference_id: str = None) -> ComplianceResponse:
    """Mapeia uma linha do DuckDB para o schema ComplianceResponse completo."""
    def clean(val, default=0.0):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return default
        return val

    return ComplianceResponse(
        reference_id=reference_id or str(row.get("reference_id", "")),
        property_id=str(row.get("property_id", "")),
        property_name=str(row.get("property_alias") or "Não Informado"),
        property_alias=str(row.get("property_alias") or "Sem Alias"),
        total_area_ha=float(clean(row.get("property_area_ha"))),
        verdict=str(row.get("final_eligibility_status", "UNKNOWN")),
        city=str(row.get("city_name", "Não Informada")),
        car_status=str(row.get("car_status", "ATIVO")),
        max_slope_degrees=float(clean(row.get("max_slope", 0.0))),
        
        environmental_score=EnvironmentalScore(
            biome=str(row.get("biome_name", "Desconhecido")),
            legal_reserve_required_pct=float(clean(row.get("legal_reserve_required_pct"))),
            has_app_area=bool(row.get("has_app", False)),
            critical_app_violation=bool(row.get("has_app_violation", False)),
            is_eudr_compliant=bool(row.get("is_eudr_compliant", False)),
            general_ndvi_mean=float(clean(row.get("ndvi_mean"))),
            app_ndvi_mean=float(clean(row.get("ndvi_app_mean"))),
            rl_deficit_ha=float(clean(row.get("legal_reserve_deficit_ha"))),
            rl_balance_ha=float(clean(row.get("legal_reserve_balance_ha")))
        ),
        
        deforestation_metrics=DeforestationMetrics(
            mapbiomas_deforested_ha=float(clean(row.get("mapbiomas_deforested_ha"))),
            eudr_deforested_ha=float(clean(row.get("eudr_deforested_ha"))),
            mapbiomas_date=row.get("mapbiomas_alert_date"),
            mapbiomas_alert_id=row.get("mapbiomas_alert_id")
        ),
        
        social_score=SocialScore(
            indigenous_land_overlap=bool(row.get("is_protected_area_overlap", False)),
            quilombola_land_overlap=bool(row.get("is_quilombola_overlap", False)),
            slave_labor_offender=bool(row.get("is_slave_labor", False))
        ),
        
        risk_analysis=RiskAnalysis(
            oldest_embargo_date=row.get("embargo_date"),
            total_embargoed_area_ha=float(clean(row.get("embargo_area_ha"))),
            adjacency_contamination_risk=bool(row.get("adjacency_risk", False)),
            technical_evidence=row.get("technical_evidence"),
            internal_risks_found=row.get("risk_summary"),
            adjacency_details=row.get("adjacency_details")
        )
    )

# --- ENDPOINTS ---

@app.get(
    "/compliance/car/{car_code}", 
    response_model=ComplianceResponse, 
    tags=["Compliance"],
    responses={404: {"description": "Código do CAR ou ID da propriedade não encontrado"}}
)
async def get_by_car(car_code: str):
    """Busca exata por Código do CAR ou ID da Propriedade."""
    query = """
        SELECT * FROM compliance_data 
        WHERE property_id = ? OR property_alias = ? 
        LIMIT 1
    """
    result_df = db_con.execute(query, [car_code, car_code]).df()
    if result_df.empty:
        raise HTTPException(status_code=404, detail="Código do CAR não encontrado na base.")
    
    row = result_df.replace({np.nan: None}).iloc[0].to_dict()
    return map_row_to_response(row)

@app.get(
    "/compliance/point", 
    response_model=ComplianceResponse, 
    tags=["Compliance"],
    responses={404: {"description": "Nenhuma propriedade encontrada para a coordenada informada"}}
)
async def get_by_point(lat: float, lon: float):
    """Busca espacial por Ponto (Lat/Lon)."""
    query = "SELECT * FROM compliance_data WHERE ST_Contains(geometry, ST_Point(?, ?)) LIMIT 1"
    result_df = db_con.execute(query, [lon, lat]).df()
    if result_df.empty:
        raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
    
    row = result_df.replace({np.nan: None}).iloc[0].to_dict()
    return map_row_to_response(row)

@app.post("/compliance/polygon", response_model=List[ComplianceResponse], tags=["Compliance"])
async def get_by_polygon(request: PolygonRequest):
    """Busca por Polígono (WKT ou GeoJSON). Retorna todas as fazendas que intersectam."""
    try:
        if request.wkt:
            geom_query = "ST_GeomFromText(?)"
            param = request.wkt
        elif request.geojson:
            geom_query = "ST_GeomFromGeoJSON(?)"
            param = json.dumps(request.geojson)
        else:
            raise HTTPException(status_code=400, detail="Forneça 'wkt' ou 'geojson'.")

        query = f"SELECT * FROM compliance_data WHERE ST_Intersects(geometry, {geom_query})"
        result_df = db_con.execute(query, [param]).df()
        
        return [map_row_to_response(row.to_dict()) for _, row in result_df.replace({np.nan: None}).iterrows()]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro no processamento da geometria: {str(e)}")

@app.post("/compliance/batch/csv", response_model=CSVUploadResponse, tags=["Compliance"])
async def process_csv_compliance(file: UploadFile = File(...)):
    """
    Processamento inteligente de CSV:
    Identifica automaticamente colunas de CAR, Coordenadas ou WKT.
    """
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content), sep=None, engine='python').head(settings.max_csv_rows)
    
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
                res_df = db_con.execute("SELECT * FROM compliance_data WHERE property_id = ? OR property_alias = ? LIMIT 1", 
                                       [str(row[car_col]), str(row[car_col])]).df()
            elif wkt_col and pd.notnull(row[wkt_col]):
                res_df = db_con.execute("SELECT * FROM compliance_data WHERE ST_Intersects(geometry, ST_GeomFromText(?)) LIMIT 1", 
                                       [str(row[wkt_col])]).df()
            elif lat_col and lon_col and pd.notnull(row[lat_col]):
                res_df = db_con.execute("SELECT * FROM compliance_data WHERE ST_Contains(geometry, ST_Point(?, ?)) LIMIT 1", 
                                       [float(row[lon_col]), float(row[lat_col])]).df()

            if not res_df.empty:
                row_dict = res_df.replace({np.nan: None}).iloc[0].to_dict()
                results.append(map_row_to_response(row_dict, reference_id=ref_id))
        except:
            continue

    return {"total_processed": len(results), "results": results}

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Verifica o status da API e do motor de processamento."""
    return {
        "status": "online", 
        "engine": "DuckDB + Spatial",
        "version": "3.1.0"
    }
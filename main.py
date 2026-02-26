import time
import io
import pandas as pd
from typing import List, Generator
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic_settings import BaseSettings

# Importações dos seus schemas locais
from schemas import (ComplianceResponse, CoordinatePoint, 
                    BatchCoordinateRequest, CSVUploadResponse)

# --- CONFIGURAÇÕES ---
class Settings(BaseSettings):
    max_csv_rows: int = 500
    bigquery_dataset: str = "agro_esg_marts.fct_compliance_risk"

    model_config = {
        "env_file": ".env",
        "extra": "ignore" 
    }

settings = Settings()

# --- APP SETUP ---
app = FastAPI(
    title="AgroMarte Sentinela API",
    description="Inteligência e Compliance Espacial - Agri Market Intelligence & Risk",
    version="2.2.1"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DEPENDÊNCIAS ---
def get_bq_client() -> Generator:
    """Gerencia o ciclo de vida do client por requisição."""
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# --- FUNÇÕES AUXILIARES ---
def map_row_to_response(row, reference_id=None):
    status = row.get("final_eligibility_status", "UNKNOWN")
    return {
        "reference_id": reference_id,
        "property_id": str(row.get("property_id", "")),
        "property_name": row.get("property_name"),
        "property_alias": row.get("property_alias"),
        "total_area_ha": float(row.get("property_area_ha", 0)),
        "verdict": status,
        "environmental_score": {
            "biome": row.get("biome_name"),
            "legal_reserve_required_pct": float(row.get("legal_reserve_req", 0)),
            "has_app_area": bool(row.get("has_app_area")),
            "critical_app_violation": "CRITICAL APP VIOLATION" in status
        },
        "social_score": {
            "indigenous_land_overlap": bool(row.get("is_indigenous_land")),
            "quilombola_land_overlap": bool(row.get("is_quilombola_land"))
        },
        "risk_analysis": {
            "oldest_embargo_date": str(row.get("final_embargo_date")) if row.get("final_embargo_date") else None,
            "total_embargoed_area_ha": float(row.get("embargo_overlap_ha", 0)),
            "adjacency_contamination_risk": "CONTAMINATION" in status
        }
    }

async def execute_batch_query(points: List[CoordinatePoint], client: bigquery.Client):
    """Lógica centralizada para consulta em lote no BigQuery."""
    query = f"""
    WITH input_points AS (
        SELECT p.lat, p.lon, p.reference_id
        FROM UNNEST(@points) AS p
    )
    SELECT 
        target.*, 
        pts.reference_id
    FROM `{settings.bigquery_dataset}` AS target
    JOIN input_points AS pts 
        ON ST_CONTAINS(target.geometry, ST_GEOGPOINT(pts.lon, pts.lat))
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY pts.reference_id 
        ORDER BY target.property_area_ha DESC
    ) = 1
    """

    # Blindagem total de tipos para o STRUCT do BigQuery
    points_param = [
        {
            "lat": float(p.lat), 
            "lon": float(p.lon), 
            "reference_id": str(p.reference_id)
        } for p in points
    ]

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "points", 
                "STRUCT<lat FLOAT64, lon FLOAT64, reference_id STRING>", 
                points_param
            )
        ]
    )

    query_job = client.query(query, job_config=job_config)
    return [map_row_to_response(dict(row), row.reference_id) for row in query_job.result()]

# --- ENDPOINTS ---

@app.get("/health", tags=["Infrastructure"])
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.post("/compliance/batch/csv", response_model=CSVUploadResponse, tags=["Compliance"])
async def process_csv_compliance(
    file: UploadFile = File(...),
    client: bigquery.Client = Depends(get_bq_client)
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")
    
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content), encoding='utf-8-sig', sep=None, engine='python')
    df_limited = df.head(settings.max_csv_rows)
    points = []

    # 1. Lógica 'coords'
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

    # 2. Fallback Lat/Lon
    if not points:
        lat_col = next((c for c in df_limited.columns if 'lat' in c.lower()), None)
        lon_col = next((c for c in df_limited.columns if 'log' in c.lower() or 'lon' in c.lower()), None)
        
        if lat_col and lon_col:
            for idx, row in df_limited.iterrows():
                try:
                    points.append(CoordinatePoint(
                        lat=float(str(row[lat_col]).replace(',', '.')),
                        lon=float(str(row[lon_col]).replace(',', '.')),
                        reference_id=str(row.get('property_alias', idx))
                    ))
                except: continue

    if not points:
        raise HTTPException(status_code=400, detail="Nenhuma coordenada válida encontrada.")

    results = await execute_batch_query(points, client)
    return {"total_processed": len(results), "results": results}

@app.post("/compliance/batch/points", response_model=List[ComplianceResponse], tags=["Compliance"])
async def get_batch_compliance_by_points(
    request: BatchCoordinateRequest, 
    client: bigquery.Client = Depends(get_bq_client)
):
    try:
        return await execute_batch_query(request.points, client)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro BigQuery: {str(e)}")

@app.get("/compliance/point", response_model=ComplianceResponse, tags=["Compliance"])
async def get_compliance_by_point(
    lat: float, lon: float, 
    client: bigquery.Client = Depends(get_bq_client)
):
    query = f"SELECT * FROM `{settings.bigquery_dataset}` WHERE ST_CONTAINS(geometry, ST_GEOGPOINT(@lon, @lat)) LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lat", "FLOAT64", lat),
            bigquery.ScalarQueryParameter("lon", "FLOAT64", lon),
        ]
    )
    results = list(client.query(query, job_config=job_config).result())
    if not results:
        raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
    
    return map_row_to_response(dict(results[0]))

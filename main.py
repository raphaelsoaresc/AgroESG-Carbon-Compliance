import time
import io
import pandas as pd
from typing import List
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery

from schemas import (
    ComplianceResponse, 
    CoordinatePoint, 
    BatchCoordinateRequest,
    CSVUploadResponse
)

app = FastAPI(
    title="AgroMarte Sentinela API",
    description="Inteligência e Compliance Espacial - Agri Market Intelligence & Risk",
    version="2.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = bigquery.Client()

@app.get("/health", tags=["Infrastructure"])
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# --- ENDPOINT CSV INTELIGENTE (DETECÇÃO POR CONTEÚDO) ---
@app.post("/compliance/batch/csv", response_model=CSVUploadResponse, tags=["Compliance"])
async def process_csv_compliance(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")

    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))

    lat_col, lon_col = None, None

    # LÓGICA SENTINELA: Varre as colunas buscando dados que pareçam coordenadas brasileiras
    for col in df.columns:
        # Tenta converter a coluna para número (descarta o que for texto puro)
        serie_numerica = pd.to_numeric(df[col], errors='coerce').dropna()
        
        if serie_numerica.empty:
            continue
            
        media = serie_numerica.mean()
        
        # Heurística para o Brasil (Lat: -35 a 6 | Lon: -75 a -33)
        if -35 <= media <= 6 and lat_col is None:
            lat_col = col
        elif -75 <= media <= -33 and lon_col is None:
            lon_col = col

    if not lat_col or not lon_col:
        raise HTTPException(
            status_code=400, 
            detail="Sentinela não detectou coordenadas. Verifique se os dados são numéricos."
        )

    # Identificação opcional de ID/Referência
    cols_normais = {c.lower(): c for c in df.columns}
    ref_col = next((cols_normais[c] for c in ['id', 'nome', 'ponto', 'reference_id'] if c in cols_normais), None)

    points = []
    # head(100) para manter a performance no BigQuery UNNEST
    for idx, row in df.head(100).iterrows():
        try:
            # Garante que estamos pegando números válidos
            lat_val = float(row[lat_col])
            lon_val = float(row[lon_col])
            
            points.append(CoordinatePoint(
                lat=lat_val,
                lon=lon_val,
                reference_id=str(row[ref_col]) if ref_col else f"linha_{idx}"
            ))
        except (ValueError, TypeError):
            continue # Pula linhas com sujeira

    results = await get_batch_compliance_by_points(BatchCoordinateRequest(points=points))
    return {"total_processed": len(results), "results": results}

# --- CONSULTA BATCH (BIGQUERY) ---
@app.post("/compliance/batch/points", response_model=List[ComplianceResponse], tags=["Compliance"])
async def get_batch_compliance_by_points(request: BatchCoordinateRequest):
    query = """
        WITH input_points AS (
            SELECT p.lat, p.lon, p.reference_id
            FROM UNNEST(@points) AS p
        )
        SELECT 
            target.*,
            pts.reference_id
        FROM `agro_esg_marts.fct_compliance_risk` AS target
        JOIN input_points AS pts 
            ON ST_CONTAINS(target.geometry, ST_GEOGPOINT(pts.lon, pts.lat))
    """
    
    points_data = [{"lat": p.lat, "lon": p.lon, "reference_id": p.reference_id} for p in request.points]
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("points", "STRUCT<lat FLOAT64, lon FLOAT64, reference_id STRING>", points_data)
        ]
    )
    
    try:
        query_job = client.query(query, job_config=job_config)
        # Retorna a lista mapeada usando sua função de mapper
        return [map_row_to_response(row, row.reference_id) for row in query_job.result()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro BigQuery: {str(e)}")

# --- CONSULTA PONTO ÚNICO ---
@app.get("/compliance/point", response_model=ComplianceResponse, tags=["Compliance"])
async def get_compliance_by_point(lat: float, lon: float):
    query = """
        SELECT * FROM `agro_esg_marts.fct_compliance_risk`
        WHERE ST_CONTAINS(geometry, ST_GEOGPOINT(@lon, @lat)) LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lat", "FLOAT64", lat),
            bigquery.ScalarQueryParameter("lon", "FLOAT64", lon),
        ]
    )
    results = client.query(query, job_config=job_config).result()
    row = next(results, None)
    if not row:
        raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
    return map_row_to_response(row)

# --- MAPPER ---
def map_row_to_response(row, reference_id=None):
    status = row.final_eligibility_status
    return {
        "reference_id": reference_id,
        "property_id": row.property_id,
        "property_name": row.property_name,
        "property_alias": row.property_alias,
        "total_area_ha": row.property_area_ha,
        "verdict": status,
        "environmental_score": {
            "biome": row.biome_name,
            "legal_reserve_required_pct": row.legal_reserve_req,
            "has_app_area": row.has_app_area,
            "critical_app_violation": "CRITICAL APP VIOLATION" in status
        },
        "social_score": {
            "indigenous_land_overlap": row.is_indigenous_land,
            "quilombola_land_overlap": row.is_quilombola_land
        },
        "risk_analysis": {
            "oldest_embargo_date": row.final_embargo_date,
            "total_embargoed_area_ha": row.embargo_overlap_ha,
            "adjacency_contamination_risk": "CONTAMINATION" in status
        }
    }

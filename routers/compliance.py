import io
import json
import pandas as pd
import numpy as np
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File

from schemas import (
    ComplianceResponse, PolygonRequest, CSVUploadResponse,
    EnvironmentalScore, DeforestationMetrics, SocialScore, RiskAnalysis, FinancialLiabilities
)
from services.billing import get_current_user, check_user_permission, register_usage, censor_response, DEMO_IDS

router = APIRouter(tags=["Compliance"])

ROBUST_GEOM_SELECT = "ST_AsGeoJSON(geometry::GEOMETRY) AS geometry"
ROBUST_GEOM_WHERE = "geometry::GEOMETRY"

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

@router.get("/compliance/point", response_model=ComplianceResponse)
async def get_by_point(lat: float, lon: float, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con # Pega a conexão injetada no main.py
    try:
        query = f"SELECT * EXCLUDE (geometry), {ROBUST_GEOM_SELECT} FROM compliance_data WHERE ST_Contains({ROBUST_GEOM_WHERE}, ST_Point(?, ?)) LIMIT 1"
        result_df = db_con.execute(query,[lon, lat]).df()
        
        if result_df.empty:
            raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
        
        row = result_df.replace({np.nan: None}).iloc[0].to_dict()
        full_response = map_row_to_response(row)
        
        if full_response.property_id in DEMO_IDS: return full_response
        if not user_email: return censor_response(full_response)
        if check_user_permission(user_email, full_response.property_id):
            register_usage(user_email, full_response.property_id)
            return full_response
        return censor_response(full_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro espacial: {str(e)}")

@router.get("/compliance/car/{car_id}", response_model=ComplianceResponse)
async def get_by_car(car_id: str, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    try:
        query = f"SELECT * EXCLUDE (geometry), {ROBUST_GEOM_SELECT} FROM compliance_data WHERE property_id = ?"
        result_df = db_con.execute(query,[car_id]).df()
        
        if result_df.empty:
            raise HTTPException(status_code=404, detail="CAR não encontrado.")

        row = result_df.replace({np.nan: None}).to_dict(orient="records")[0]
        full_response = map_row_to_response(row)

        if car_id in DEMO_IDS: return full_response
        if not user_email: return censor_response(full_response)
        if check_user_permission(user_email, car_id):
            register_usage(user_email, car_id)
            return full_response
        return censor_response(full_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/compliance/polygon", response_model=List[ComplianceResponse])
async def get_by_polygon(payload: PolygonRequest, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    try:
        if payload.wkt and payload.wkt.strip():
            geom_query = "ST_GeomFromText(?)"
            param = payload.wkt
        elif payload.geojson:
            geom_query = "ST_GeomFromGeoJSON(?)"
            param = json.dumps(payload.geojson)
        else:
            raise HTTPException(status_code=400, detail="Geometria inválida.")

        query = f"SELECT * EXCLUDE (geometry), {ROBUST_GEOM_SELECT} FROM compliance_data WHERE ST_Intersects({ROBUST_GEOM_WHERE}, {geom_query})"
        result_df = db_con.execute(query, [param]).df()
        
        results =[]
        for _, row in result_df.replace({np.nan: None}).iterrows():
            full_response = map_row_to_response(row.to_dict())
            car_id = full_response.property_id
            if car_id in DEMO_IDS or (user_email and check_user_permission(user_email, car_id)):
                if user_email and car_id not in DEMO_IDS: register_usage(user_email, car_id)
                results.append(full_response)
            else:
                results.append(censor_response(full_response))
        return results
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro na geometria: {str(e)}")

@router.post("/compliance/batch/csv", response_model=CSVUploadResponse)
async def process_csv_compliance(request: Request, file: UploadFile = File(...), user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    content = await file.read()
    df = pd.read_csv(io.BytesIO(content)).head(500)
    
    results =[]
    car_col = next((c for c in df.columns if 'car' in c.lower() or 'property_id' in c.lower()), None)
    
    for idx, row in df.iterrows():
        try:
            if car_col and pd.notnull(row[car_col]):
                q = f"SELECT * EXCLUDE (geometry), {ROBUST_GEOM_SELECT} FROM compliance_data WHERE property_id = ? LIMIT 1"
                res_df = db_con.execute(q, [str(row[car_col])]).df()
                if not res_df.empty:
                    row_dict = res_df.replace({np.nan: None}).iloc[0].to_dict()
                    full_response = map_row_to_response(row_dict)
                    car_id = full_response.property_id
                    if car_id in DEMO_IDS or (user_email and check_user_permission(user_email, car_id)):
                        results.append(full_response)
                    else:
                        results.append(censor_response(full_response))
        except: continue
    return {"total_processed": len(results), "results": results}
import io
import json
import ast
import pandas as pd
import numpy as np
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File
import requests

from schemas import (
    ComplianceResponse, PolygonRequest, CSVUploadResponse,
    EnvironmentalScore, DeforestationMetrics, SocialScore, RiskAnalysis, FinancialLiabilities
)
from services.billing import get_current_user, check_user_permission, register_usage, censor_response, DEMO_IDS

router = APIRouter(tags=["Compliance"])

TABLE_NAME = "fct_compliance_latest" # Nome da tabela final do Mart
GEOMETRY_TABLE = "fct_compliance_geometries" # Nome da tabela de geometrias

def map_row_to_response(row: dict, reference_id: str = None) -> ComplianceResponse:
    
    def clean_num(val, default=0.0):
        if val is None or pd.isna(val) or str(val).lower() in ("none", "nan", "null"):
            return default
        try: return float(val)
        except: return default

    def clean_bool(val):
        if val is None or pd.isna(val): return False
        if isinstance(val, bool): return val
        return str(val).lower() in ("true", "1", "t", "y", "yes")

    def clean_str(val, default=None):
        if val is None or pd.isna(val) or str(val).lower() in ("none", "null"): 
            return default
        return str(val).strip()

    def parse_complex_field(field):
        if field is None or pd.isna(field): return None
        if isinstance(field, (dict, list)): return field
        field_str = str(field).strip()
        if not field_str: return None
        try: return json.loads(field_str)
        except:
            try: return ast.literal_eval(field_str)
            except: return field_str

    return ComplianceResponse(
        property_id=clean_str(row.get("property_id")),
        property_alias=clean_str(row.get("property_alias"), "Sem Alias"),
        area_ha=clean_num(row.get("area_ha")),
        property_area_ha=clean_num(row.get("property_area_ha")),
        city=clean_str(row.get("city"), "Não Informada"),
        uf_origem=clean_str(row.get("uf_origem"), "N/A"),
        car_status=clean_str(row.get("car_status"), "ATIVO"),
        
        geometry=parse_complex_field(row.get("geometry_json")),
        car_bbox=parse_complex_field(row.get("car_bbox")),
        latitude=clean_num(row.get("latitude")),
        longitude=clean_num(row.get("longitude")),
        max_slope_degrees=clean_num(row.get("max_slope_degrees")),
        relief_classification=clean_str(row.get("relief_classification"), "N/A"),
        
        final_eligibility_status=clean_str(row.get("final_eligibility_status"), "UNKNOWN"),
        is_technically_blocked=clean_bool(row.get("is_technically_blocked")),
        geospatial_confidence_level=clean_str(row.get("geospatial_confidence_level"), "LOW"),
        
        is_settlement_identity=clean_bool(row.get("is_settlement_identity")),
        is_traditional_identity=clean_bool(row.get("is_traditional_identity")),
        is_quilombo_identity=clean_bool(row.get("is_quilombo_identity")),
        producer_size_category=clean_str(row.get("producer_size_category"), "N/A"),
        is_small_holder=clean_bool(row.get("is_small_holder")),
        
        analyzed_at=None if pd.isna(row.get("analyzed_at")) else row.get("analyzed_at"),
        processed_at=None if pd.isna(row.get("processed_at")) else row.get("processed_at"),
        
        financial_liabilities=FinancialLiabilities(
            estimated_financial_liability_brl=clean_num(row.get("estimated_financial_liability_brl")),
            liability_deforestation_brl=clean_num(row.get("liability_deforestation_brl")),
            liability_rl_brl=clean_num(row.get("liability_rl_brl")),
            liability_protected_areas_brl=clean_num(row.get("liability_protected_areas_brl")),
            liability_social_brl=clean_num(row.get("liability_social_brl")),
            liability_embargo_brl=clean_num(row.get("liability_embargo_brl")),
            liability_app_brl=clean_num(row.get("liability_app_brl"))
        ),
        
        environmental_score=EnvironmentalScore(
            biome_name=clean_str(row.get("biome_name"), "Desconhecido"),
            car_on_car_overlap_pct=clean_num(row.get("car_on_car_overlap_pct")),
            is_eudr_restricted=clean_bool(row.get("is_eudr_restricted")),
            rl_status=clean_str(row.get("rl_status"), "N/A"),
            rl_deficit_ha=clean_num(row.get("rl_deficit_ha")),
            rl_balance_ha=clean_num(row.get("rl_balance_ha")),
            fmp_ha=clean_num(row.get("fmp_ha")),
            historical_warnings=clean_str(row.get("historical_warnings"))
        ),
        
        deforestation_metrics=DeforestationMetrics(
            mapbiomas_deforested_ha=clean_num(row.get("mapbiomas_deforested_ha")),
            eudr_deforested_ha=clean_num(row.get("eudr_deforested_ha")),
            deforestation_types=parse_complex_field(row.get("deforestation_types")),
            official_reports_urls=parse_complex_field(row.get("official_reports_urls")),
            evidence_date_before=None if pd.isna(row.get("evidence_date_before")) else row.get("evidence_date_before"),
            evidence_date_after=None if pd.isna(row.get("evidence_date_after")) else row.get("evidence_date_after"),
            mapbiomas_detection_date=None if pd.isna(row.get("mapbiomas_detection_date")) else row.get("mapbiomas_detection_date"),
            mapbiomas_alert_ids=clean_str(row.get("mapbiomas_alert_ids")),
            official_alert_area_ha=clean_num(row.get("official_alert_area_ha"))
        ),
        
        social_score=SocialScore(
            is_protected_area_overlap=clean_bool(row.get("is_protected_area_overlap")),
            protected_area_overlap_ha=clean_num(row.get("protected_area_overlap_ha")),
            slave_labor_overlap_ha=clean_num(row.get("slave_labor_overlap_ha")),
            slave_labor_inclusion_date=None if pd.isna(row.get("slave_labor_inclusion_date")) else row.get("slave_labor_inclusion_date"),
            forensic_ti_ha=clean_num(row.get("forensic_ti_ha")),
            forensic_quilombo_ha=clean_num(row.get("forensic_quilombo_ha")),
            forensic_uc_ha=clean_num(row.get("forensic_uc_ha")),
            forensic_settlement_ha=clean_num(row.get("forensic_settlement_ha")),
            forensic_traditional_ha=clean_num(row.get("forensic_traditional_ha"))
        ),
        
        risk_analysis=RiskAnalysis(
            embargo_area_ha=clean_num(row.get("embargo_area_ha")),
            is_embargo_active=clean_bool(row.get("is_embargo_active")),
            embargo_offenders=clean_str(row.get("embargo_offenders")),
            embargo_processes=clean_str(row.get("embargo_processes")),
            embargo_tax_ids=clean_str(row.get("embargo_tax_ids")),
            embargo_reported_areas=clean_str(row.get("embargo_reported_areas")),
            embargo_sources_string=clean_str(row.get("embargo_sources_string")),
            embargo_date=None if pd.isna(row.get("embargo_date")) else row.get("embargo_date"),
            internal_risks_found=clean_str(row.get("internal_risks_found")),
            adjacency_details=clean_str(row.get("adjacency_details")),
            technical_evidence=clean_str(row.get("technical_evidence")),
            max_adjacency_score=clean_num(row.get("max_adjacency_score"))
        ),

        geom_car_total=parse_complex_field(row.get("geom_car_total")),
        geom_embargos=parse_complex_field(row.get("geom_embargos")),
        geom_desmatamento=parse_complex_field(row.get("geom_desmatamento")),
        geom_eudr=parse_complex_field(row.get("geom_eudr")),
        geom_areas_protegidas=parse_complex_field(row.get("geom_areas_protegidas")),
        geom_assentamentos=parse_complex_field(row.get("geom_assentamentos")),
        geom_conflito_app=parse_complex_field(row.get("geom_conflito_app"))
    )

def build_compliance_query(where_clause: str) -> str:
    return f"""
        WITH pivoted_geoms AS (
            SELECT 
                property_id,
                ANY_VALUE(CASE WHEN map_layer = 'PROPERTY_BOUNDARY' THEN ST_AsGeoJSON(geometry) END) AS geom_car_total,
                ANY_VALUE(CASE WHEN map_layer = 'RESTRICTION_EMBARGO' THEN ST_AsGeoJSON(geometry) END) AS geom_embargos,
                ANY_VALUE(CASE WHEN map_layer = 'RESTRICTION_DEFORESTATION' THEN ST_AsGeoJSON(geometry) END) AS geom_desmatamento,
                ANY_VALUE(CASE WHEN map_layer = 'RESTRICTION_EUDR' THEN ST_AsGeoJSON(geometry) END) AS geom_eudr,
                ANY_VALUE(CASE WHEN map_layer = 'RESTRICTION_SOCIAL_ENVIRONMENTAL' THEN ST_AsGeoJSON(geometry) END) AS geom_areas_protegidas,
                ANY_VALUE(CASE WHEN target_type = 'RECORTE_INVASAO_ASSENTAMENTO' THEN ST_AsGeoJSON(geometry) END) AS geom_assentamentos,
                ANY_VALUE(CASE WHEN map_layer = 'RESTRICTION_APP' THEN ST_AsGeoJSON(geometry) END) AS geom_conflito_app
            FROM {GEOMETRY_TABLE}
            GROUP BY property_id
        )
        SELECT 
            c.* EXCLUDE (geometry), 
            ST_AsGeoJSON(c.geometry) AS geometry_json,
            g.* EXCLUDE (property_id)
        FROM {TABLE_NAME} c
        LEFT JOIN pivoted_geoms g ON c.property_id = g.property_id
        WHERE {where_clause}
    """

@router.get("/compliance/point", response_model=ComplianceResponse)
async def get_by_point(lat: float, lon: float, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    try:
        query = build_compliance_query("ST_Within(ST_Point(?, ?), c.geometry)")
        result_df = db_con.execute(query, [lon, lat]).df()
        if result_df.empty:
            raise HTTPException(status_code=404, detail="Coordenada fora de áreas mapeadas.")
        row = result_df.replace({np.nan: None}).iloc[0].to_dict()
        return map_row_to_response(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro espacial: {str(e)}")

@router.get("/compliance/car/{car_id}", response_model=ComplianceResponse)
async def get_by_car(car_id: str, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    try:
        query = build_compliance_query("c.property_id = ?")
        result_df = db_con.execute(query, [car_id]).df()
        if result_df.empty:
            raise HTTPException(status_code=404, detail="CAR não encontrado.")
        row = result_df.replace({np.nan: None}).to_dict(orient="records")[0]
        return map_row_to_response(row)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.post("/compliance/polygon", response_model=List[ComplianceResponse])
async def get_by_polygon(payload: PolygonRequest, request: Request, user_email: Optional[str] = Depends(get_current_user)):
    db_con = request.app.state.db_con
    try:
        if payload.wkt:
            geom_query, param = "ST_GeomFromText(?)", payload.wkt
        elif payload.geojson:
            geom_query, param = "ST_GeomFromGeoJSON(?)", json.dumps(payload.geojson)
        else:
            raise HTTPException(status_code=400, detail="Geometria inválida.")
        
        query = build_compliance_query(f"ST_Intersects(c.geometry, {geom_query})")
        result_df = db_con.execute(query, [param]).df()
        
        final_results = []
        for _, row in result_df.replace({np.nan: None}).iterrows():
            res = map_row_to_response(row.to_dict())
            car_id = res.property_id
            
            if car_id in DEMO_IDS:
                final_results.append(res)
            elif not user_email:
                final_results.append(censor_response(res))
            elif check_user_permission(user_email, car_id):
                register_usage(user_email, car_id)
                final_results.append(res)
            else:
                final_results.append(censor_response(res))
                
        return final_results
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro na geometria: {str(e)}")

@router.get("/compliance/list")
async def list_properties(
    request: Request,
    status: Optional[str] = None,
    biome: Optional[str] = None,
    uf: Optional[str] = None,
    city: Optional[str] = None,
    producer_type: Optional[str] = None,
    confidence: Optional[str] = None,
    limit: int = 12,
    offset: int = 0
):
    db_con = request.app.state.db_con
    filters = ["1=1"]
    params = []

    if status == 'CONFORME_VERDE':
        filters.append("final_eligibility_status LIKE 'ELIGIBLE%'")
    elif status == 'REVISAO_AZUL':
        filters.append("final_eligibility_status LIKE 'MANUAL_REVIEW%' OR final_eligibility_status LIKE 'AWAITING%'")
    elif status == 'BLOQUEADO_VERMELHO':
        filters.append("final_eligibility_status LIKE 'NOT ELIGIBLE%'")
    elif status == 'ALERTA_LARANJA':
        filters.append("final_eligibility_status LIKE 'WARNING%' OR final_eligibility_status LIKE 'CONDITIONAL%'")

    if biome: filters.append("biome_name = ?"); params.append(biome)
    if uf: filters.append("uf_origem = ?"); params.append(uf.upper())
    if city: filters.append("city = ?"); params.append(city)
    if producer_type: filters.append("producer_size_category = ?"); params.append(producer_type)
    if confidence: filters.append("geospatial_confidence_level = ?"); params.append(confidence)

    where_clause = " AND ".join(filters)
    
    count_query = f"SELECT COUNT(*) as total FROM {TABLE_NAME} WHERE {where_clause}"
    total_count = db_con.execute(count_query, params).df()['total'][0]

    query = f"""
        SELECT property_id, area_ha, city, uf_origem, biome_name,
               final_eligibility_status, geospatial_confidence_level, producer_size_category
        FROM {TABLE_NAME}
        WHERE {where_clause}
        ORDER BY analyzed_at DESC
        LIMIT {limit} OFFSET {offset}
    """
    df = db_con.execute(query, params).df()
    
    return {
        "items": df.to_dict(orient="records"),
        "total": int(total_count)
    }

@router.get("/compliance/filter-options")
async def get_filter_options(
    request: Request, 
    biome: Optional[str] = None, 
    uf: Optional[str] = None,
    city: Optional[str] = None,
    producer_type: Optional[str] = None,
    confidence: Optional[str] = None,
    car_status: Optional[str] = None
):
    db_con = request.app.state.db_con
    
    active_filters = {
        "biome_name": biome,
        "uf_origem": uf,
        "city": city,
        "producer_size_category": producer_type,
        "geospatial_confidence_level": confidence,
        "car_status": car_status
    }

    def get_distinct_options(target_column):
        filters = ["1=1"]
        params = []
        
        for col, val in active_filters.items():
            if val and col != target_column:
                filters.append(f"{col} = ?")
                params.append(val)
        
        where_clause = " AND ".join(filters)
        query = f"SELECT DISTINCT {target_column} FROM {TABLE_NAME} WHERE {target_column} IS NOT NULL AND {where_clause} ORDER BY 1"
        return db_con.execute(query, params).df()[target_column].tolist()

    try:
        return {
            "biomes": get_distinct_options("biome_name"),
            "ufs": get_distinct_options("uf_origem"),
            "cities": get_distinct_options("city"),
            "producer_types": get_distinct_options("producer_size_category"),
            "confidences": get_distinct_options("geospatial_confidence_level"),
            "car_statuses": get_distinct_options("car_status")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
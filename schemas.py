from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime

class FinancialLiabilities(BaseModel):
    estimated_financial_liability_brl: float = 0.0
    liability_deforestation_brl: float = 0.0
    liability_rl_brl: float = 0.0
    liability_protected_areas_brl: float = 0.0
    liability_social_brl: float = 0.0
    liability_app_brl: float = 0.0
    liability_embargo_brl: float = 0.0  # Campo incluído para embargo

class EnvironmentalScore(BaseModel):
    biome_name: Optional[str] = None
    car_on_car_overlap_pct: float = 0.0
    is_eudr_restricted: bool = False
    rl_status: Optional[str] = None
    rl_deficit_ha: float = 0.0
    rl_balance_ha: float = 0.0
    fmp_ha: float = 0.0  # Campo incluído
    historical_warnings: Optional[str] = None

class DeforestationMetrics(BaseModel):
    mapbiomas_deforested_ha: float = 0.0
    eudr_deforested_ha: float = 0.0
    deforestation_types: Optional[str] = None
    official_reports_urls: Optional[str] = None
    evidence_date_before: Optional[date] = None
    evidence_date_after: Optional[date] = None
    mapbiomas_detection_date: Optional[date] = None

class SocialScore(BaseModel):
    is_protected_area_overlap: bool = False
    protected_area_overlap_ha: float = 0.0
    protected_overlap_ha: float = 0.0
    slave_labor_overlap_ha: float = 0.0
    slave_labor_inclusion_date: Optional[date] = None
    # Campos Periciais incluídos
    forensic_ti_ha: float = 0.0
    forensic_quilombo_ha: float = 0.0
    forensic_uc_ha: float = 0.0
    forensic_settlement_ha: float = 0.0
    forensic_traditional_ha: float = 0.0

class RiskAnalysis(BaseModel):
    embargo_area_ha: float = 0.0
    is_embargo_active: bool = False
    embargo_offenders: Optional[str] = None
    embargo_processes: Optional[str] = None
    embargo_tax_ids: Optional[str] = None
    embargo_reported_areas: Optional[str] = None
    embargo_sources_string: Optional[str] = None
    embargo_date: Optional[date] = None
    internal_risks_found: Optional[str] = None
    adjacency_details: Optional[str] = None
    technical_evidence: Optional[str] = None

class ComplianceResponse(BaseModel):
    # Identificação e Localização
    property_id: str
    property_alias: Optional[str] = None
    area_ha: float = 0.0
    property_area_ha: float = 0.0
    city: Optional[str] = None
    uf_origem: Optional[str] = None
    car_status: Optional[str] = None
    
    # Geometria e Espacial
    geometry: Optional[Dict[str, Any]] = None
    car_bbox: Optional[Dict[str, Any]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_slope_degrees: float = 0.0
    relief_classification: Optional[str] = None

    # Flags de Identidade e Categoria de Produtor (Campos Faltantes Adicionados)
    is_settlement_identity: bool = False
    is_traditional_identity: bool = False
    is_quilombo_identity: bool = False
    producer_size_category: Optional[str] = "N/A"
    is_small_holder: bool = False
    
    # Status de Elegibilidade
    final_eligibility_status: Optional[str] = None
    is_technically_blocked: bool = False
    geospatial_confidence_level: Optional[str] = None
    
    # Metadados de Processamento
    analyzed_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    
    # Blocos de Dados (Agrupados para organização)
    financial_liabilities: FinancialLiabilities
    environmental_score: EnvironmentalScore
    deforestation_metrics: DeforestationMetrics
    social_score: SocialScore
    risk_analysis: RiskAnalysis

    # Novos campos de geometria para mapas
    geom_car_total: Optional[dict] = None
    geom_embargos: Optional[dict] = None
    geom_desmatamento: Optional[dict] = None
    geom_areas_protegidas: Optional[dict] = None
    geom_assentamentos: Optional[dict] = None
    geom_conflito_app: Optional[dict] = None

class PolygonRequest(BaseModel):
    wkt: Optional[str] = None
    geojson: Optional[Dict] = None
    reference_id: Optional[str] = None

class CSVUploadResponse(BaseModel):
    total_processed: int
    results: List[ComplianceResponse]
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import date, datetime

class FinancialLiabilities(BaseModel):
    estimated_financial_liability_brl: float = 0.0
    liability_deforestation_brl: float = 0.0
    liability_rl_brl: float = 0.0
    liability_protected_areas_brl: float = 0.0
    liability_social_brl: float = 0.0
    liability_app_brl: float = 0.0
    liability_embargo_brl: float = 0.0

class EnvironmentalScore(BaseModel):
    biome_name: Optional[str] = None
    car_on_car_overlap_pct: float = 0.0
    is_eudr_restricted: bool = False
    rl_status: Optional[str] = None
    rl_deficit_ha: float = 0.0
    rl_balance_ha: float = 0.0
    fmp_ha: float = 0.0
    historical_warnings: Optional[str] = None
    is_liability_uncertain: bool = False 
    solicitacao_adesao_pra: Optional[str] = None
    area_rural_consolidada_ha: float = 0.0
    area_pousio_ha: float = 0.0
    area_uso_restrito_ha: float = 0.0
    forensic_app_hidrica_ha: float = 0.0
    forensic_app_declividade_ha: float = 0.0
    is_area_inconsistent: bool = False
    # Nova coluna: Marco Legal
    reference_forest_code_date: Optional[date] = None

class DeforestationMetrics(BaseModel):
    mapbiomas_deforested_ha: float = 0.0
    # Nova coluna: Valor Bruto
    mapbiomas_deforested_ha_raw: float = 0.0
    eudr_deforested_ha: float = 0.0
    deforestation_types: Optional[str] = None
    official_reports_urls: Optional[str] = None
    evidence_date_before: Optional[date] = None
    evidence_date_after: Optional[date] = None
    mapbiomas_detection_date: Optional[date] = None
    mapbiomas_alert_ids: Optional[str] = None
    official_alert_area_ha: float = 0.0

class SocialScore(BaseModel):
    is_protected_area_overlap: bool = False
    protected_area_overlap_ha: float = 0.0
    slave_labor_overlap_ha: float = 0.0
    slave_labor_inclusion_date: Optional[date] = None
    forensic_ti_ha: float = 0.0
    forensic_quilombo_ha: float = 0.0
    forensic_uc_ha: float = 0.0
    forensic_settlement_ha: float = 0.0
    forensic_traditional_ha: float = 0.0
    ti_overlap_pct: float = 0.0
    uc_overlap_pct: float = 0.0
    settlement_overlap_pct: float = 0.0
    traditional_overlap_pct: float = 0.0
    mapbiomas_official_ti_ha: float = 0.0
    mapbiomas_official_quilombo_ha: float = 0.0
    mapbiomas_official_settlement_ha: float = 0.0
    # Novas colunas: Filtros de BI (Nomes)
    ti_name: Optional[str] = None
    uc_name: Optional[str] = None
    settlement_name: Optional[str] = None
    quilombo_name: Optional[str] = None
    traditional_name: Optional[str] = None

class RiskAnalysis(BaseModel):
    embargo_area_ha: float = 0.0
    # Nova coluna: Valor Bruto
    embargo_area_ha_raw: float = 0.0
    is_embargo_active: bool = False
    # Nova coluna: Flag Regulatória
    is_cmn_5081_sensitive: bool = False
    embargo_offenders: Optional[str] = None
    embargo_processes: Optional[str] = None
    embargo_tax_ids: Optional[str] = None
    embargo_reported_areas: Optional[str] = None
    embargo_sources_string: Optional[str] = None
    embargo_date: Optional[date] = None
    internal_risks_found: Optional[str] = None
    adjacency_details: Optional[str] = None
    technical_evidence: Optional[str] = None
    max_adjacency_score: float = 0.0
    city_data_source_origin: Optional[str] = None
    adjacent_roads: Optional[str] = None
    logistics_risk_level: Optional[str] = None
    logistics_risk_score: int = 0
    is_structured_environmental_risk: bool = False
    count_artificial_water_bodies: int = 0
    artificial_water_details: Optional[str] = None
    has_physical_barrier: bool = False
    adjacent_rivers: Optional[str] = None
    road_overlap_ha: float = 0.0
    road_names: Optional[str] = None
    evidence_admin: Optional[str] = None
    evidence_social: Optional[str] = None
    evidence_environmental: Optional[str] = None
    evidence_infrastructure: Optional[str] = None
    # Novas colunas: Dados Estruturados (Arrays)
    evidence_admin_array: List[Any] = []
    evidence_social_array: List[Any] = []
    evidence_environmental_array: List[Any] = []
    evidence_infrastructure_array: List[Any] = []

class ComplianceResponse(BaseModel):
    property_id: str
    property_alias: Optional[str] = None
    property_identity_type: Optional[str] = None
    area_ha: float = 0.0
    area_geometria_ha: float
    property_area_ha: float = 0.0
    area_liquida_ha: float = 0.0
    fiscal_modules: float = 0.0
    city: Optional[str] = None
    uf_origem: Optional[str] = None
    car_status: Optional[str] = None
    # Nova coluna: Auditoria de Status
    car_status_spatial: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    # Nova coluna: Dados Geográficos
    centroid: Optional[Dict[str, Any]] = None
    car_bbox: Optional[Dict[str, Any]] = None
    critical_contact_point: Optional[Dict[str, Any]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_slope_degrees: float = 0.0
    relief_classification: Optional[str] = None
    is_settlement_identity: bool = False
    is_traditional_identity: bool = False
    is_quilombo_identity: bool = False
    is_ti_identity: bool = False
    is_uc_identity: bool = False
    producer_size_category: Optional[str] = "N/A"
    is_small_holder: bool = False
    final_eligibility_status: Optional[str] = None
    final_eligibility_status_detailed: Optional[str] = None
    is_technically_blocked: bool = False
    is_missing_geometry: bool = False
    geospatial_confidence_level: Optional[str] = None
    data_reliability_index: int = 0
    data_source_quality: Optional[str] = None
    forensic_summary: Optional[str] = None
    analyzed_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    financial_liabilities: FinancialLiabilities
    environmental_score: EnvironmentalScore
    deforestation_metrics: DeforestationMetrics
    social_score: SocialScore
    risk_analysis: RiskAnalysis
    geom_car_total: Optional[dict] = None
    geom_embargos: Optional[dict] = None
    geom_desmatamento: Optional[dict] = None
    geom_eudr: Optional[dict] = None
    geom_areas_protegidas: Optional[dict] = None
    geom_assentamentos: Optional[dict] = None
    geom_conflito_app: Optional[dict] = None
    geom_adjacencia_risco: Optional[dict] = None

# ESTAS CLASSES DEVEM ESTAR NO FINAL DO ARQUIVO
class PolygonRequest(BaseModel):
    wkt: Optional[str] = None
    geojson: Optional[Dict] = None
    reference_id: Optional[str] = None

class CSVUploadResponse(BaseModel):
    total_processed: int
    results: List[ComplianceResponse]
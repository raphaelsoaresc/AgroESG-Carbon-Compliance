from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date

class EnvironmentalScore(BaseModel):
    biome: str
    legal_reserve_required_pct: float
    has_app_area: bool
    critical_app_violation: bool
    is_eudr_compliant: bool = Field(..., description="Elegibilidade para exportação UE (pós-2020)")
    # Novos campos adicionados:
    general_ndvi_mean: float = Field(..., description="Média de NDVI geral da propriedade")
    app_ndvi_mean: float = Field(..., description="Média de NDVI especificamente nas áreas de APP")
    rl_deficit_ha: float = Field(..., description="Déficit de Reserva Legal em hectares")
    rl_balance_ha: float = Field(..., description="Saldo de Reserva Legal em hectares")

class DeforestationMetrics(BaseModel):
    mapbiomas_deforested_ha: float = Field(..., description="Área desmatada segundo MapBiomas (ha)")
    eudr_deforested_ha: float = Field(..., description="Área desmatada pós-2020 (critério EUDR)")
    mapbiomas_date: Optional[date] = Field(None, description="Data do alerta de desmatamento")
    mapbiomas_alert_id: Optional[str] = Field(None, description="ID do alerta no MapBiomas")

class SocialScore(BaseModel):
    indigenous_land_overlap: bool
    quilombola_land_overlap: bool
    slave_labor_offender: bool = Field(..., description="Verificação de presença na 'Lista Suja' de trabalho escravo")

class RiskAnalysis(BaseModel):
    oldest_embargo_date: Optional[date]
    total_embargoed_area_ha: float
    adjacency_contamination_risk: bool
    # Novos campos adicionados:
    technical_evidence: Optional[str] = Field(None, description="Evidências técnicas coletadas (links ou descrições)")
    internal_risks_found: Optional[str] = Field(None, description="Resumo de riscos internos identificados")
    adjacency_details: Optional[str] = Field(None, description="Detalhes sobre riscos em propriedades vizinhas")

class CoordinatePoint(BaseModel):
    lat: float
    lon: float
    reference_id: Optional[str] = Field(None, description="ID para identificar o ponto")

class BatchCoordinateRequest(BaseModel):
    points: List[CoordinatePoint] = Field(..., max_items=100)

class ComplianceResponse(BaseModel):
    reference_id: Optional[str] = None 
    property_id: str
    property_name: str
    property_alias: str
    total_area_ha: float
    verdict: str
    # Novos campos de Informação da Propriedade:
    city: str
    car_status: str = Field(..., description="Status do CAR (Ativo, Pendente, Suspenso)")
    max_slope_degrees: float = Field(..., description="Declividade máxima encontrada (importante para crédito agrícola)")
    
    # Blocos de métricas
    environmental_score: EnvironmentalScore
    deforestation_metrics: DeforestationMetrics # Novo bloco incluído
    social_score: SocialScore
    risk_analysis: RiskAnalysis

class CSVUploadResponse(BaseModel):
    total_processed: int
    results: List[ComplianceResponse]
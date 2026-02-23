from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date

class EnvironmentalScore(BaseModel):
    biome: str
    legal_reserve_required_pct: float
    has_app_area: bool
    critical_app_violation: bool

class SocialScore(BaseModel):
    indigenous_land_overlap: bool
    quilombola_land_overlap: bool

class RiskAnalysis(BaseModel):
    oldest_embargo_date: Optional[date]
    total_embargoed_area_ha: float
    adjacency_contamination_risk: bool

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
    environmental_score: EnvironmentalScore
    social_score: SocialScore
    risk_analysis: RiskAnalysis

class CSVUploadResponse(BaseModel):
    total_processed: int
    results: List[ComplianceResponse]

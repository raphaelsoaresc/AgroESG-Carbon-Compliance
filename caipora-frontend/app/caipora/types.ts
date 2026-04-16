// Interfaces auxiliares baseadas nos sub-modelos do Pydantic
export interface FinancialLiabilities {
  estimated_financial_liability_brl: number;
  liability_deforestation_brl: number;
  liability_rl_brl: number;
  liability_protected_areas_brl: number;
  liability_social_brl: number;
  liability_app_brl: number;
  liability_embargo_brl: number;
}

export interface EnvironmentalScore {
  biome_name?: string;
  car_on_car_overlap_pct: number;
  is_eudr_restricted: boolean;
  rl_status?: string;
  rl_deficit_ha: number;
  rl_balance_ha: number;
  fmp_ha: number;
  historical_warnings?: string;
  is_liability_uncertain: boolean;
  solicitacao_adesao_pra?: string;
  area_rural_consolidada_ha: number;
  area_pousio_ha: number;
  area_uso_restrito_ha: number;
  forensic_app_hidrica_ha: number;
  forensic_app_declividade_ha: number;
  is_area_inconsistent: boolean;
  reference_forest_code_date?: string; // ISO Date
}

export interface DeforestationMetrics {
  mapbiomas_deforested_ha: number;
  mapbiomas_deforested_ha_raw: number;
  eudr_deforested_ha: number;
  deforestation_types?: string;
  official_reports_urls?: string;
  evidence_date_before?: string; // ISO Date
  evidence_date_after?: string;  // ISO Date
  mapbiomas_detection_date?: string; // ISO Date
  mapbiomas_alert_ids?: string;
  official_alert_area_ha: number;
}

export interface SocialScore {
  is_protected_area_overlap: boolean;
  protected_area_overlap_ha: number;
  slave_labor_overlap_ha: number;
  slave_labor_inclusion_date?: string;
  forensic_ti_ha: number;
  forensic_quilombo_ha: number;
  forensic_uc_ha: number;
  forensic_settlement_ha: number;
  forensic_traditional_ha: number;
  ti_overlap_pct: number;
  uc_overlap_pct: number;
  settlement_overlap_pct: number;
  traditional_overlap_pct: number;
  mapbiomas_official_ti_ha: number;
  mapbiomas_official_quilombo_ha: number;
  mapbiomas_official_settlement_ha: number;
  ti_name?: string;
  uc_name?: string;
  settlement_name?: string;
  quilombo_name?: string;
  traditional_name?: string;
}

export interface RiskAnalysis {
  embargo_area_ha: number;
  embargo_area_ha_raw: number;
  is_embargo_active: boolean;
  is_cmn_5081_sensitive: boolean;
  embargo_offenders?: string;
  embargo_processes?: string;
  embargo_tax_ids?: string;
  embargo_reported_areas?: string;
  embargo_sources_string?: string;
  embargo_date?: string;
  internal_risks_found?: string;
  adjacency_details?: string;
  technical_evidence?: string;
  max_adjacency_score: number;
  city_data_source_origin?: string;
  adjacent_roads?: string;
  logistics_risk_level?: string;
  logistics_risk_score: number;
  is_structured_environmental_risk: boolean;
  count_artificial_water_bodies: number;
  artificial_water_details?: string;
  has_physical_barrier: boolean;
  adjacent_rivers?: string;
  road_overlap_ha: number;
  road_names?: string;
  evidence_admin?: string;
  evidence_social?: string;
  evidence_environmental?: string;
  evidence_infrastructure?: string;
  evidence_admin_array: any[];
  evidence_social_array: any[];
  evidence_environmental_array: any[];
  evidence_infrastructure_array: any[];
}

// Interface Principal (AuditData)
export interface AuditData {
  // 1. Identificação Básica
  propertyId: string;
  propertyAlias?: string;
  property_identity_type?: string;
  area_ha: number;
  area_geometria_ha: number;
  property_area_ha: number;
  area_liquida_ha: number;
  fiscal_modules: number;
  city?: string;
  uf_origem?: string;
  car_status?: string;
  car_status_spatial?: string;
  
  // 2. Localização e Perícia
  latitude?: number;
  longitude?: number;
  geometry?: any;
  centroid?: any;
  car_bbox?: any;
  critical_contact_point?: any;
  max_slope_degrees: number;
  relief_classification?: string;

  // Campos de Imagem para o PDF (Injetados via Frontend)
  map_image_satellite?: string | null; 
  map_image_environmental?: string | null;
  
  // 3. Identidades e Categorias
  is_settlement_identity: boolean;
  is_traditional_identity: boolean;
  is_quilombo_identity: boolean;
  is_ti_identity: boolean;
  is_uc_identity: boolean;
  producer_size_category?: string;
  is_small_holder: boolean;

  // 4. Status e Confiança
  final_eligibility_status?: string;
  final_eligibility_status_detailed?: string;
  is_technically_blocked: boolean;
  is_missing_geometry: boolean;
  geospatial_confidence_level?: string;
  data_reliability_index: number;
  data_source_quality?: string;
  forensic_summary?: string;
  analyzed_at?: string;
  processed_at?: string;

  // 5. Objetos Complexos
  financial_liabilities: FinancialLiabilities;
  environmental_score: EnvironmentalScore;
  deforestation_metrics: DeforestationMetrics;
  social_score: SocialScore;
  risk_analysis: RiskAnalysis;

  // 6. Geometrias Adicionais
  geom_car_total?: any;
  geom_embargos?: any;
  geom_desmatamento?: any;
  geom_eudr?: any;
  geom_areas_protegidas?: any;
  geom_assentamentos?: any;
  geom_conflito_app?: any;
  geom_adjacencia_risco?: any;

  // --- UI HELPERS ---
  carNumber?: string; 
  status?: string;    
  color?: 'red' | 'orange' | 'green' | 'blue';
  metrics?: string;
  isCensored?: boolean;
  mapCenterCoords?: [number, number];
  evidenceList?: string[];
  
  liabilityTotal?: string;
  liabilityAmbientalTotal?: string;
}

export interface UseAuditReturn {
  isAdmin: boolean;
  logout: () => void;
  carId: string;
  setCarId: (id: string) => void;
  performSearch: (id: string) => Promise<void>;
  searchCount: number;
  data: AuditData | null;
  loading: boolean;
  limitReached: boolean;
  showPayment: boolean;
  preferenceId: string | null;
  handleUnlockReport: () => void;
}

export const DEMO_IDS = [
  "PA-1505304-BB9F3EB9FBCD498BB2F07BB62EE58B4E", // Risco Crítico
  "AM-1301704-5F6515F5B115419F9114891B2D3A2C54", // Alerta
  "RO-1100205-D3628A9E4A1542CFBA0BCDEF40828540", // Revisão
  "MT-5101605-503D1E16017740379EE73DB43DE1358C"  // Conforme
];

// types.ts - VERSÃO FINAL SINCRONIZADA
export interface AuditData {
  // Identificação e Localização
  propertyId: string;
  propertyAlias?: string;
  carNumber: string; // Adicionado para resolver o erro ts(2353)
  area: number;      // Adicionado
  areaHa: number;
  propertyAreaHa: number;
  city: string;
  uf: string;
  latitude: number;
  longitude: number;
  carBbox: any;
  mapCenterCoords: [number, number];

  // Perícia Física e Identidades
  maxSlopeDegrees: number;
  relief: string;
  isSettlementIdentity: boolean;
  isTraditionalIdentity: boolean;
  isQuilomboIdentity: boolean;
  isTiIdentity: boolean;
  isUcIdentity: boolean;
  isSmallHolder: boolean;
  producerSizeCategory: string;

  // Status e Elegibilidade
  status: string; 
  statusDetailed: string;
  finalEligibilityStatus: string;         // Adicionado
  finalEligibilityStatusDetailed: string; // Adicionado
  carStatus: string;
  isTechnicallyBlocked: boolean;
  confidenceLevel: string;
  geospatialConfidenceLevel: string;
  dataReliabilityIndex: number;
  forensicSummary: string;
  analysisReason: string;
  processedAt: string;
  analyzedAt: string;

  // Sub-objetos Brutos da API
  financialLiabilities: any;
  environmentalScore: any;
  deforestationMetrics: any;
  socialScore: any;
  riskAnalysis: any;

  // Passivos Formatados (UI)
  liabilityTotal: string;
  liabilityAmbientalTotal: string;
  liabilityDeforestation: string;
  liabilitySocial: string;
  liabilityRL: string;
  liabilityProtected: string;
  liabilityAPP: string;
  liabilityEmbargo: string;
  
  // Ambiental e Risco
  biomeName?: string;
  rlStatus?: string;
  isLiabilityUncertain: boolean;
  historicalWarnings?: string;
  embargoProcesses?: string;
  embargoOffenders?: string;
  mapbiomasUrl: string | null;
  mapbiomasAlertIds?: string;
  liabilityDeforestationHa?: number; // Adicionado
  maxAdjacencyScore: number;
  adjacentRoads?: string;
  cityDataSourceOrigin?: string;

  // Gavetas de Evidências
  evidenceList: string[];
  evidenceAdmin?: string;
  evidenceSocial?: string;
  evidenceEnvironmental?: string;
  evidenceInfrastructure?: string;
  internalRisks?: string;
  
  // UI e Mapa
  metrics: string;
  color: 'red' | 'orange' | 'green' | 'blue';
  isCensored: boolean;

  // Geometrias (GeoJSON)
  geometry: any;
  geom_car_total?: any;
  geom_embargos?: any;
  geom_desmatamento?: any;
  geom_eudr?: any;
  geom_areas_protegidas?: any;
  geom_conflito_app?: any;
  geom_assentamentos?: any;
  geom_adjacencia_risco?: any;
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

export interface HeaderProps {
  carId: string;
  setCarId: (id: string) => void;
  onSearch: (id?: string) => void;
  searchCount: number;
  isAdmin: boolean;
  logout: () => void;
}

export const DEMO_IDS = [
  "PA-1505304-BB9F3EB9FBCD498BB2F07BB62EE58B4E", // Risco Crítico
  "AM-1300706-913A813EECC74CA5A6132C87A2937749", // Alerta
  "RO-1100205-92C5EDA2BBA94FB6B85684DD3F701D48", // Revisão
  "MT-5107800-89AAA1E222A548B581D6081C93232B12"  // Conforme
];

// types.ts

export interface AuditData {
  status: string;
  analysisReason: string;
  carStatus: string;
  processedAt: string;
  analyzedAt: string;
  mapbiomasUrl: string | null;
  relief: string;
  carNumber: string;
  uf: string;
  city: string;
  area: number;
  
  // Financeiro
  liabilityTotal: string;
  liabilityAmbientalTotal: string;
  liabilityDeforestation: string;
  liabilitySocial: string;
  liabilityRL: string;
  liabilityProtected: string;
  liabilityAPP: string;
  liabilityEmbargo: string;
  
  // Conteúdo
  evidenceList: string[];
  metrics: string;
  color: 'red' | 'orange' | 'green' | 'blue';
  isCensored: boolean;
  confidenceLevel: string;
  
  // Novos campos de Inteligência
  isTechnicallyBlocked: boolean;
  producerSizeCategory: string;
  internalRisks?: string;
  historicalWarnings?: string;
  embargoProcesses?: string;
  embargoOffenders?: string;

  // Geometrias
  mapCenterCoords: [number, number];
  geometry: any;
  geom_embargos?: any;
  geom_desmatamento?: any;
  geom_areas_protegidas?: any;
  geom_conflito_app?: any;
  geom_assentamentos?: any;
}

// Interface para o retorno do Hook useAudit
export interface UseAuditReturn {
  isAdmin: boolean;
  logout: () => void;
  carId: string;
  setCarId: (id: string) => void;
  performSearch: (id?: string) => Promise<void>;
  searchCount: number;
  data: AuditData | null;
  loading: boolean;
  limitReached: boolean;
  showPayment: boolean;
  preferenceId: string | null;
  handleUnlockReport: () => void;
}

// Interface para as Props do componente Header
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

export interface AuditData {
  status: string;
  liabilityTotal: string;
  liabilityDeforestation: string;
  liabilitySocial: string;
  uf: string;
  city: string;
  area: number;
  protectedOverlap: number;
  mapbiomasAlertId?: string;
  evidenceList: string[];
  metrics: string;
  color: 'red' | 'orange' | 'green';
  mapCenterCoords: [number, number];
  polygonCoords: [number, number][];
  isCensored: boolean;
}

export const DEMO_IDS = [
  'MT-5107859-9DFDE64A2FFC4556B116F9BDE0C6595F',
  'AM-1303569-85EECD549EC34411BEBF5142E59E304A',
  'PA-1503754-5E969C33E8D14256A06C6452F71A113D'
];
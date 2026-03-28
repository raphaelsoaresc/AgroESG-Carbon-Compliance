export const parseGeometry = (geom: any): [number, number][] => {
  if (!geom) return [];
  try {
    if (geom.type === 'Polygon') return geom.coordinates[0].map((pt: any) => [pt[1], pt[0]]);
    if (geom.type === 'MultiPolygon') return geom.coordinates[0][0].map((pt: any) => [pt[1], pt[0]]);
  } catch (e) { 
    return []; 
  }
  return [];
};

export const formatCurrency = (value: number) => 
  new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(value);

export const cleanEvidence = (rawEvidence: string) => {
  return rawEvidence
    .replace(/\[.*?\]/g, '') 
    .split('|')
    .map((s: string) => s.trim())
    .filter((s: string) => s.length > 5 && !s.includes('Vizinho com'));
};
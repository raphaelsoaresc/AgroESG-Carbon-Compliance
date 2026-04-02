// utils.ts

export const parseGeometry = (geom: any): any => {
  if (!geom) return [];

  // Caso o backend envie como string (JSON stringificado pelo DuckDB)
  const g = typeof geom === 'string' ? JSON.parse(geom) : geom;

  try {
    if (g.type === 'Polygon') {
      // Retorna [ [lat, lng], [lat, lng], ... ]
      return g.coordinates[0].map((pt: any) => [pt[1], pt[0]]);
    }
    
    if (g.type === 'MultiPolygon') {
      // Retorna [ [[lat, lng], ...], [[lat, lng], ...] ]
      return g.coordinates.map((poly: any) => 
        poly[0].map((pt: any) => [pt[1], pt[0]])
      );
    }
  } catch (e) {
    console.error("Erro ao processar geometria:", e);
    return [];
  }
  return [];
};

/**
 * Extrai o primeiro ponto válido para centralizar o mapa
 * Garante o retorno do tipo [number, number] para satisfazer o TypeScript
 */
export const getCenterPoint = (coords: any): [number, number] => {
  const fallback: [number, number] = [-15, -55];

  if (!coords || !Array.isArray(coords) || coords.length === 0) {
    return fallback;
  }

  try {
    // Caso seja MultiPolygon: coords[0] é um array de pontos [[lat, lng], ...]
    if (Array.isArray(coords[0]) && Array.isArray(coords[0][0])) {
      const point = coords[0][0];
      return [point[0], point[1]] as [number, number];
    }

    // Caso seja Polygon simples: coords[0] é um ponto [lat, lng]
    if (Array.isArray(coords[0]) && typeof coords[0][0] === 'number') {
      return [coords[0][0], coords[0][1]] as [number, number];
    }
  } catch (e) {
    return fallback;
  }

  return fallback;
};

export const formatCurrency = (value: number) => 
  new Intl.NumberFormat('pt-BR', { 
    style: 'currency', 
    currency: 'BRL', 
    maximumFractionDigits: 0 
  }).format(value);

export const cleanEvidence = (rawEvidence: string) => {
  if (!rawEvidence) return [];
  return rawEvidence
    .replace(/\[.*?\]/g, '') 
    .split('|')
    .map((s: string) => s.trim())
    .filter((s: string) => s.length > 5 && !s.includes('Vizinho com'));
};
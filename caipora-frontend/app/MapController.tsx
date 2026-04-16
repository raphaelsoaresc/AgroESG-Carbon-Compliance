import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import { ensureIterable } from "./map-utils";

export const MapController = ({ data }: { data: any }) => {
  const map = useMap();
  const lastIdRef = useRef<string | null>(null);
  const currentId = data?.propertyId ?? data?.property_id ?? "initial";

  useEffect(() => {
    if (!map || !data) return;
    if (lastIdRef.current === currentId) return;

    try {
      // Usamos any aqui para evitar o conflito de união do TypeScript com o Leaflet
      let bounds: any = null;

      // 1. Processar car_bbox (Prioridade 1)
      if (data.car_bbox) {
        const b = data.car_bbox;
        // Se for [minLon, minLat, maxLon, maxLat]
        if (Array.isArray(b) && b.length === 4) {
          bounds = [[b[1], b[0]], [b[3], b[2]]];
        } 
        // Se já for [[lat, lon], [lat, lon]]
        else if (Array.isArray(b) && b.length === 2) {
          bounds = b;
        }
      }

      // 2. Calcular da geometria (Prioridade 2)
      if (!bounds) {
        const geometry = ensureIterable(data.geom_car_total || data.geometry);
        if (geometry) {
          const geojsonLayer = L.geoJSON(geometry);
          bounds = geojsonLayer.getBounds();
        }
      }

      // 3. Validar e Aplicar
      if (bounds) {
        // O "as any" aqui resolve o erro TS2345, permitindo que o Leaflet processe a união
        const finalBounds = L.latLngBounds(bounds as any);
        
        if (finalBounds.isValid()) {
          map.fitBounds(finalBounds, { 
            padding: [50, 50], 
            maxZoom: 18,
            animate: false 
          });
          lastIdRef.current = currentId;
        }
      }
    } catch (err) {
      console.error("Erro ao ajustar bounds:", err);
      // Fallback para centralização simples
      if (data.latitude && data.longitude) {
        map.setView([data.latitude, data.longitude], 15);
        lastIdRef.current = currentId;
      }
    }
  }, [data, map, currentId]);

  return null;
};
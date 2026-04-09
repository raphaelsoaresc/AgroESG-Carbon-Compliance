import { useEffect, useRef } from "react";
import { useMap } from "react-leaflet";
import L from "leaflet";
import { ensureIterable } from "./map-utils";

export const MapController = ({ data }: { data: any }) => {
  const map = useMap();
  const lastIdRef = useRef<string | null>(null);
  const currentId = data?.property_id ?? data?.carNumber ?? "initial";

  useEffect(() => {
    const geometry = ensureIterable(data?.geometry);
    if (!map || !geometry) return;

    // SÓ move a câmera se o ID mudar. Se for o mesmo imóvel, não faz NADA.
    if (lastIdRef.current === currentId) return;

    try {
      const geojsonLayer = L.geoJSON(geometry);
      const bounds = geojsonLayer.getBounds();

      if (bounds.isValid()) {
        map.fitBounds(bounds, { 
          padding: [50, 50], 
          maxZoom: 18, 
          animate: false // Sem animação para evitar saltos
        });
        lastIdRef.current = currentId;
      }
    } catch (err) {
      console.error("Erro ao ajustar bounds:", err);
    }
  }, [data, map, currentId]);

  return null;
};
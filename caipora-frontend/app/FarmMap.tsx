"use client";

import React, { useEffect } from "react";
import { MapContainer, TileLayer, GeoJSON, useMap, LayersControl } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

if (typeof window !== "undefined") {
  // @ts-ignore
  delete L.Icon.Default.prototype._getIconUrl;
  L.Icon.Default.mergeOptions({
    iconRetinaUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon-2x.png",
    iconUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-icon.png",
    shadowUrl: "https://unpkg.com/leaflet@1.7.1/dist/images/marker-shadow.png",
  });
}

interface FarmMapProps {
  data: any;
}

// Função auxiliar para garantir que o dado seja um objeto GeoJSON válido
const ensureIterable = (geo: any) => {
  if (!geo) return null;
  try {
    return typeof geo === 'string' ? JSON.parse(geo) : geo;
  } catch (e) {
    return null;
  }
};

const MapController = ({ data }: { data: any }) => {
  const map = useMap();

  useEffect(() => {
    const geometry = ensureIterable(data?.geometry);
    if (!map || !geometry) return;

    map.whenReady(() => {
      try {
        const geojsonLayer = L.geoJSON(geometry);
        const bounds = geojsonLayer.getBounds();

        if (bounds.isValid()) {
          const timer = setTimeout(() => {
            if (map.getContainer()) {
              map.invalidateSize();
              map.fitBounds(bounds, { padding: [30, 30], maxZoom: 16 });
            }
          }, 200);
          return () => clearTimeout(timer);
        }
      } catch (err) {
        console.error("Erro ao ajustar o mapa:", err);
      }
    });
  }, [data, map]);

  return null;
};

const FarmMap = ({ data }: FarmMapProps) => {
  if (!data) return <div className="h-full w-full flex items-center justify-center bg-gray-100">Aguardando dados...</div>;

  // Geramos uma chave única baseada no ID do imóvel para forçar o React a redesenhar os polígonos
  const mapKey = data.carNumber || "initial";

  return (
    <div className="h-full w-full relative" style={{ minHeight: "400px" }}>
      <MapContainer
        center={[data.latitude || -15, data.longitude || -50]}
        zoom={4}
        scrollWheelZoom={true}
        className="h-full w-full"
      >
        <LayersControl position="topright">
          <LayersControl.BaseLayer checked name="Satélite">
            <TileLayer
              url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
              attribution="Esri"
            />
          </LayersControl.BaseLayer>
          
          <LayersControl.BaseLayer name="OpenStreetMap">
            <TileLayer
              url="https://{s}.tile.openstreetmap.org/{z}/{y}/{x}.png"
              attribution="&copy; OSM"
            />
          </LayersControl.BaseLayer>

          {/* Camada Principal: Limite do Imóvel */}
          {data.geometry && (
            <LayersControl.Overlay checked name="Limite do Imóvel (CAR)">
              <GeoJSON 
                key={`main-${mapKey}`}
                data={ensureIterable(data.geometry)} 
                style={{ color: "#3b82f6", weight: 3, fillOpacity: 0.1 }} 
              />
            </LayersControl.Overlay>
          )}

          {/* Camadas Adicionais com chaves únicas para atualização em tempo real */}
          {data.geom_embargos && (
            <LayersControl.Overlay name="Áreas Embargadas">
              <GeoJSON 
                key={`embargo-${mapKey}`}
                data={ensureIterable(data.geom_embargos)} 
                style={{ color: "#ef4444", weight: 2, fillColor: "#ef4444", fillOpacity: 0.5 }} 
              />
            </LayersControl.Overlay>
          )}

          {data.geom_desmatamento && (
            <LayersControl.Overlay name="Desmatamento">
              <GeoJSON 
                key={`desmat-${mapKey}`}
                data={ensureIterable(data.geom_desmatamento)} 
                style={{ color: "#f97316", weight: 2, fillColor: "#f97316", fillOpacity: 0.5 }} 
              />
            </LayersControl.Overlay>
          )}

          {data.geom_areas_protegidas && (
            <LayersControl.Overlay name="Áreas Protegidas">
              <GeoJSON 
                key={`prot-${mapKey}`}
                data={ensureIterable(data.geom_areas_protegidas)} 
                style={{ color: "#10b981", weight: 2, fillColor: "#10b981", fillOpacity: 0.4 }} 
              />
            </LayersControl.Overlay>
          )}

          {data.geom_conflito_app && (
            <LayersControl.Overlay name="Conflito APP">
              <GeoJSON 
                key={`app-${mapKey}`}
                data={ensureIterable(data.geom_conflito_app)} 
                style={{ color: "#8b5cf6", weight: 2, fillColor: "#8b5cf6", fillOpacity: 0.4 }} 
              />
            </LayersControl.Overlay>
          )}

          {data.geom_assentamentos && (
            <LayersControl.Overlay name="Assentamentos">
              <GeoJSON 
                key={`assent-${mapKey}`}
                data={ensureIterable(data.geom_assentamentos)} 
                style={{ color: "#facc15", weight: 2, fillColor: "#facc15", fillOpacity: 0.4 }} 
              />
            </LayersControl.Overlay>
          )}
        </LayersControl>

        <MapController data={data} />
      </MapContainer>
    </div>
  );
};

export default FarmMap;
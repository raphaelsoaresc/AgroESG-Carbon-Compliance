"use client";

import React, { useEffect, useMemo } from "react";
import { MapContainer, TileLayer, GeoJSON, useMap, LayersControl, Popup } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

// Correção de ícones para o Leaflet no Next.js
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

// Função auxiliar robusta para validar geometrias
const ensureIterable = (geo: any) => {
  if (!geo) return null;
  try {
    const parsed = typeof geo === 'string' ? JSON.parse(geo) : geo;
    return (parsed?.type === "Polygon" || parsed?.type === "MultiPolygon") ? parsed : null;
  } catch (e) {
    return null;
  }
};

// Controlador de Zoom e Enquadramento Inteligente
const MapController = ({ data }: { data: any }) => {
  const map = useMap();

  useEffect(() => {
    const geometry = ensureIterable(data?.geometry);
    if (!map || !geometry) return;

    try {
      const geojsonLayer = L.geoJSON(geometry);
      const bounds = geojsonLayer.getBounds();

      if (bounds.isValid()) {
        map.fitBounds(bounds, { 
          padding: [50, 50], 
          maxZoom: 18, 
          animate: true 
        });
      }
    } catch (err) {
      console.error("Erro ao ajustar bounds do mapa:", err);
    }
  }, [data, map]);

  return null;
};

const FarmMap = ({ data }: FarmMapProps) => {
  // Verificação de segurança inicial
  if (!data || typeof data !== 'object') return (
    <div className="h-full w-full flex items-center justify-center bg-slate-900 text-white rounded-xl">
      <div className="animate-pulse flex flex-col items-center gap-4">
        <div className="w-12 h-12 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin"></div>
        <span className="text-sm font-medium tracking-widest uppercase">Sincronizando Satélites...</span>
      </div>
    </div>
  );

  // Chave única para forçar re-renderização quando mudar de imóvel
  const mapKey = data?.property_id ?? data?.carNumber ?? "initial";

  // Lógica de Estilo Dinâmico (Sincronizado com o Motor de Risco)
  const mainStyle = useMemo(() => {
    let color = "#10b981"; // Verde (Eligible)
    let weight = 3;
    let dashArray = "";

    if (data?.final_eligibility_status?.includes("NOT ELIGIBLE")) {
      color = "#ef4444"; // Vermelho (Bloqueado)
    }
    
    if ((data?.max_adjacency_score ?? 0) > 0) {
      color = "#f97316"; // Laranja (Risco de Adjacência)
      dashArray = "10, 10"; 
      weight = 4;
    }

    return { color, weight, fillOpacity: 0.05, dashArray };
  }, [data]);

  return (
    <div className="h-full w-full relative rounded-xl overflow-hidden border border-slate-700 shadow-2xl bg-slate-900">
      <MapContainer
        center={[data?.latitude ?? -15, data?.longitude ?? -50]}
        zoom={15}
        scrollWheelZoom={true}
        className="h-full w-full"
        zoomControl={false}
      >
        <LayersControl position="topright">
          {/* Camadas de Satélite de Alta Resolução */}
          <LayersControl.BaseLayer checked name="Google Satélite (Híbrido)">
            <TileLayer
              url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}"
              maxZoom={22}
              attribution="&copy; Google Maps"
            />
          </LayersControl.BaseLayer>
          
          <LayersControl.BaseLayer name="Esri World Imagery">
            <TileLayer
              url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
              maxZoom={19}
              attribution="&copy; Esri"
            />
          </LayersControl.BaseLayer>

          {/* 1. Limite do Imóvel (CAR) */}
          <LayersControl.Overlay checked name="Limite do Imóvel (CAR)">
            <GeoJSON 
              key={`main-${mapKey}`}
              data={ensureIterable(data?.geometry)} 
              style={mainStyle}
            >
              <Popup>
                <div className="min-w-[220px] font-sans p-1">
                  <h3 className="font-bold text-slate-900 border-b border-slate-200 pb-1 mb-2">
                    {data?.property_alias ?? "Propriedade s/ Alias"}
                  </h3>
                  <div className="space-y-1.5 text-[11px]">
                    <p><span className="font-semibold text-slate-500 uppercase">Status:</span> <span className={data?.is_technically_blocked ? "text-red-600 font-bold" : "text-emerald-600 font-bold"}>{data?.final_eligibility_status ?? "N/A"}</span></p>
                    <p><span className="font-semibold text-slate-500 uppercase">Área Total:</span> {data?.area_ha?.toFixed(2) ?? "0.00"} ha</p>
                    <p><span className="font-semibold text-slate-500 uppercase">Município:</span> {data?.city ?? "N/A"} - {data?.uf_origem ?? ""}</p>
                    <p><span className="font-semibold text-slate-500 uppercase">Passivo Est.:</span> <span className="text-red-500 font-bold">R$ {data?.financial_liabilities?.estimated_financial_liability_brl?.toLocaleString('pt-BR') ?? "0,00"}</span></p>
                  </div>
                  <div className="mt-3 pt-2 border-t border-slate-100 flex justify-between items-center">
                     <span className="text-[9px] text-slate-400 italic">
                        ID: {(data?.property_id ?? data?.carNumber ?? "N/A").substring(0, 15)}...
                     </span>
                     <a href={`https://www.google.com/maps/search/?api=1&query=${data?.latitude},${data?.longitude}`} target="_blank" className="text-[10px] text-blue-600 font-bold hover:underline">Ver no Google Maps</a>
                  </div>
                </div>
              </Popup>
            </GeoJSON>
          </LayersControl.Overlay>

          {/* 2. Camada EUDR */}
          {data?.geom_eudr && (
            <LayersControl.Overlay checked name="⚠️ Restrição EUDR (Pós-2020)">
              <GeoJSON 
                key={`eudr-${mapKey}`}
                data={ensureIterable(data?.geom_eudr)} 
                style={{ color: "#d946ef", weight: 2, fillColor: "#d946ef", fillOpacity: 0.6 }} 
              >
                <Popup>
                  <div className="p-1 max-w-[180px]">
                    <p className="text-xs font-bold text-magenta-600 mb-1">🚫 RESTRIÇÃO EUDR</p>
                    <p className="text-[10px] text-slate-600 leading-tight">Desmatamento detectado após 31/12/2020. Inapto para exportação UE.</p>
                    <p className="text-[10px] mt-2 font-semibold">Área Afetada: {data?.deforestation_metrics?.eudr_deforested_ha?.toFixed(2) ?? "0.00"} ha</p>
                  </div>
                </Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* 3. Desmatamento MapBiomas */}
          {data?.geom_desmatamento && (
            <LayersControl.Overlay checked name="Desmatamento (MapBiomas)">
              <GeoJSON 
                key={`desmat-${mapKey}`}
                data={ensureIterable(data?.geom_desmatamento)} 
                style={{ color: "#facc15", weight: 1, fillColor: "#facc15", fillOpacity: 0.5 }} 
              >
                <Popup>
                  <div className="p-1 min-w-[180px]">
                    <p className="text-xs font-bold text-amber-600 mb-1">🌳 ALERTA DE DESMATAMENTO</p>
                    <div className="text-[10px] space-y-1">
                      <p><span className="font-semibold">ID Alerta:</span> {data?.deforestation_metrics?.mapbiomas_alert_ids ?? "N/A"}</p>
                      <p><span className="font-semibold">Data Detecção:</span> {data?.deforestation_metrics?.mapbiomas_detection_date ? new Date(data.deforestation_metrics.mapbiomas_detection_date).toLocaleDateString('pt-BR') : "N/A"}</p>
                      <p><span className="font-semibold">Área Detectada:</span> {data?.deforestation_metrics?.mapbiomas_deforested_ha?.toFixed(2) ?? "0.00"} ha</p>
                    </div>
                    {data?.deforestation_metrics?.official_reports_urls && (
                      <a href={data.deforestation_metrics.official_reports_urls} target="_blank" className="block mt-2 text-[10px] bg-amber-100 text-amber-700 p-1.5 rounded text-center font-bold hover:bg-amber-200 transition-colors">ACESSAR LAUDO OFICIAL</a>
                    )}
                  </div>
                </Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* 4. Embargos Oficiais */}
          {data?.geom_embargos && (
            <LayersControl.Overlay checked name="Embargos (IBAMA/SEMA)">
              <GeoJSON 
                key={`embargo-${mapKey}`}
                data={ensureIterable(data?.geom_embargos)} 
                style={{ color: "#ef4444", weight: 2, fillColor: "#ef4444", fillOpacity: 0.7, dashArray: "5, 5" }} 
              >
                <Popup>
                  <div className="p-1">
                    <p className="text-xs font-bold text-red-600 mb-1">🚫 EMBARGO ATIVO</p>
                    <div className="text-[10px] space-y-1">
                      <p><span className="font-semibold">Fonte:</span> {data?.risk_analysis?.embargo_sources_string ?? "N/A"}</p>
                      <p><span className="font-semibold">Área Embargada:</span> {data?.risk_analysis?.embargo_area_ha?.toFixed(2) ?? "0.00"} ha</p>
                    </div>
                  </div>
                </Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* 5. Áreas Protegidas */}
          {data?.geom_areas_protegidas && (
            <LayersControl.Overlay name="Sobreposição Áreas Protegidas">
              <GeoJSON 
                key={`prot-${mapKey}`}
                data={ensureIterable(data?.geom_areas_protegidas)} 
                style={{ color: "#0ea5e9", weight: 2, fillColor: "#0ea5e9", fillOpacity: 0.4 }} 
              />
            </LayersControl.Overlay>
          )}

          {/* 6. Conflito em APP */}
          {data?.geom_conflito_app && (
            <LayersControl.Overlay name="Conflito em APP">
              <GeoJSON 
                key={`app-${mapKey}`}
                data={ensureIterable(data?.geom_conflito_app)} 
                style={{ color: "#8b5cf6", weight: 2, fillColor: "#8b5cf6", fillOpacity: 0.5 }} 
              />
            </LayersControl.Overlay>
          )}
        </LayersControl>

        <MapController data={data} />
      </MapContainer>

      {/* Legenda Flutuante */}
      <div className="absolute bottom-6 left-6 z-[1000] bg-slate-900/90 backdrop-blur-md p-4 rounded-xl border border-slate-700 text-[10px] text-white shadow-2xl pointer-events-none">
        <h4 className="font-bold mb-2 text-slate-400 uppercase tracking-wider">Legenda de Compliance</h4>
        <div className="space-y-2">
          <div className="flex items-center gap-3">
            <div className="w-3 h-3 rounded-full bg-[#10b981] shadow-[0_0_8px_#10b981]"></div> <span>Elegível</span>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-3 h-3 rounded-full bg-[#ef4444] shadow-[0_0_8px_#ef4444]"></div> <span>Bloqueado</span>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-3 h-3 rounded-full border-2 border-dashed border-[#f97316]"></div> <span>Risco Adjacência</span>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-3 h-3 rounded-full bg-[#d946ef] shadow-[0_0_8px_#d946ef]"></div> <span>Restrição EUDR</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FarmMap;
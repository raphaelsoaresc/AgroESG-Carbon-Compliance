"use client";

import React, { useMemo } from "react";
import { MapContainer, TileLayer, GeoJSON, LayersControl, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { ExternalLink } from "lucide-react";

import { AuditData } from "./caipora/types";
import { ensureIterable } from "./map-utils";
import { MapController } from "./MapController";
import { MapLegend } from "./MapLegend";

interface FarmMapProps {
  data: AuditData | any;
}

const FarmMap = ({ data }: FarmMapProps) => {
  if (!data || typeof data !== 'object') return (
    <div className="h-full w-full flex items-center justify-center bg-slate-900 text-white rounded-xl min-h-[400px]">
      <div className="animate-pulse flex flex-col items-center gap-4">
        <div className="w-12 h-12 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin"></div>
        <span className="text-sm font-medium tracking-widest uppercase">Sincronizando Satélites...</span>
      </div>
    </div>
  );

  // 1. Memoização das Geometrias: Isso impede que o Leaflet re-desenhe e "pule"
  const geoMain = useMemo(() => ensureIterable(data?.geometry), [data?.geometry]);
  const geoEmbargo = useMemo(() => ensureIterable(data?.geom_embargos), [data?.geom_embargos]);
  const geoDesmat = useMemo(() => ensureIterable(data?.geom_desmatamento), [data?.geom_desmatamento]);
  const geoEudr = useMemo(() => ensureIterable(data?.geom_eudr), [data?.geom_eudr]);
  const geoAdj = useMemo(() => ensureIterable(data?.geom_adjacencia_risco), [data?.geom_adjacencia_risco]);
  const geoProt = useMemo(() => ensureIterable(data?.geom_areas_protegidas), [data?.geom_areas_protegidas]);
  const geoApp = useMemo(() => ensureIterable(data?.geom_conflito_app), [data?.geom_conflito_app]);
  const geoSoc = useMemo(() => ensureIterable(data?.geom_assentamentos), [data?.geom_assentamentos]);

  const propertyId = data?.property_id ?? data?.carNumber ?? "initial";

  const mainStyle = useMemo(() => {
    let color = "#10b981"; 
    let weight = 3;
    let dashArray = "";

    if (data?.status?.includes("NOT ELIGIBLE") || data?.final_eligibility_status?.includes("NOT ELIGIBLE")) {
      color = "#ef4444";
    }
    
    if ((data?.maxAdjacencyScore ?? (data?.risk_analysis?.max_adjacency_score ?? 0)) > 0) {
      color = "#f97316";
      dashArray = "10, 10"; 
      weight = 4;
    }

    return { color, weight, fillOpacity: 0.05, dashArray, smoothFactor: 0 } as any;
  }, [data]);

  return (
    <div className="h-full w-full relative rounded-xl overflow-hidden border border-slate-700 shadow-2xl bg-slate-900">
      <MapContainer
        center={[data?.latitude ?? -15, data?.longitude ?? -50]}
        zoom={15}
        scrollWheelZoom={true}
        className="h-full w-full"
        zoomControl={true}
        // Removido preferCanvas para evitar erro de clearRect
        // Removido key para manter a instância do mapa viva e estável
      >
        <LayersControl position="topright">
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

          {/* 1. Limite do Imóvel */}
          <LayersControl.Overlay checked name="Limite do Imóvel (CAR)">
            <GeoJSON 
              key={`main-${propertyId}`}
              data={geoMain} 
              style={mainStyle}
            >
              <Popup>
                <div className="min-w-[240px] font-sans p-1">
                  <h3 className="font-bold text-slate-900 border-b border-slate-200 pb-1 mb-2">
                    {data?.propertyAlias ?? (data?.property_alias ?? (data?.carNumber ?? "Propriedade"))}
                  </h3>
                  <div className="space-y-1.5 text-[11px]">
                    <p><span className="font-semibold text-slate-500 uppercase">Status:</span> <span className={(data?.isTechnicallyBlocked || data?.is_technically_blocked) ? "text-red-600 font-bold" : "text-emerald-600 font-bold"}>{data?.status ?? (data?.final_eligibility_status ?? "N/A")}</span></p>
                    <p><span className="font-semibold text-slate-500 uppercase">Confiabilidade:</span> <span className="text-blue-600 font-bold">{data?.dataReliabilityIndex ?? (data?.data_reliability_index ?? 0)}/100</span></p>
                    <p><span className="font-semibold text-slate-500 uppercase">Área Total:</span> {(data?.area ?? data?.property_area_ha)?.toFixed(2) ?? "0.00"} ha</p>
                    <p><span className="font-semibold text-slate-500 uppercase">Município:</span> {data?.city ?? "N/A"} ({(data?.cityDataSourceOrigin ?? data?.risk_analysis?.city_data_source_origin) || 'Oficial'})</p>
                    <p><span className="font-semibold text-slate-500 uppercase">Passivo Est.:</span> <span className="text-red-500 font-bold">{data?.liabilityTotal ?? (data?.financial_liabilities?.estimated_financial_liability_brl ? `R$ ${data.financial_liabilities.estimated_financial_liability_brl.toLocaleString('pt-BR')}` : "R$ 0,00")}</span></p>
                  </div>
                  <div className="mt-3 pt-2 border-t border-slate-100 flex justify-between items-center">
                     <span className="text-[9px] text-slate-400 italic">
                        ID: {(data?.carNumber ?? (data?.property_id ?? "N/A")).substring(0, 15)}...
                     </span>
                     <a 
                        href={`https://www.google.com/maps/search/?api=1&query=${data?.latitude},${data?.longitude}`} 
                        target="_blank" 
                        className="text-[10px] text-blue-600 font-bold hover:underline flex items-center gap-1"
                     >
                        <ExternalLink size={10} /> Ver no Google Maps
                     </a>
                  </div>
                </div>
              </Popup>
            </GeoJSON>
          </LayersControl.Overlay>

          {/* 2. Embargos */}
          {geoEmbargo && (
            <LayersControl.Overlay checked name="Embargos (IBAMA/SEMA)">
              <GeoJSON 
                key={`embargo-${propertyId}`}
                data={geoEmbargo} 
                style={{ color: "#ef4444", weight: 2, fillColor: "#ef4444", fillOpacity: 0.7, dashArray: "5, 5", smoothFactor: 0 } as any} 
              />
            </LayersControl.Overlay>
          )}

          {/* 3. Desmatamento */}
          {geoDesmat && (
            <LayersControl.Overlay checked name="Desmatamento (MapBiomas)">
              <GeoJSON 
                key={`desmat-${propertyId}`}
                data={geoDesmat} 
                style={{ color: "#facc15", weight: 1, fillColor: "#facc15", fillOpacity: 0.5, smoothFactor: 0 } as any} 
              />
            </LayersControl.Overlay>
          )}

          {/* 4. EUDR */}
          {geoEudr && (
            <LayersControl.Overlay checked name="⚠️ Restrição EUDR (Pós-2020)">
              <GeoJSON 
                key={`eudr-${propertyId}`}
                data={geoEudr} 
                style={{ color: "#d946ef", weight: 2, fillColor: "#d946ef", fillOpacity: 0.6, smoothFactor: 0 } as any} 
              />
            </LayersControl.Overlay>
          )}

          {/* 5. Adjacência */}
          {geoAdj && (
            <LayersControl.Overlay checked name="🏠 Vizinhos de Risco">
              <GeoJSON 
                key={`adj-${propertyId}`} 
                data={geoAdj} 
                style={{ color: "#f97316", weight: 2, fillColor: "#f97316", fillOpacity: 0.3, dashArray: "5, 5", smoothFactor: 0 } as any}
              />
            </LayersControl.Overlay>
          )}

          {/* 6. Áreas Protegidas */}
          {geoProt && (
            <LayersControl.Overlay name="Sobreposição Áreas Protegidas">
              <GeoJSON key={`prot-${propertyId}`} data={geoProt} style={{ color: "#0ea5e9", weight: 2, fillColor: "#0ea5e9", fillOpacity: 0.4, smoothFactor: 0 } as any} />
            </LayersControl.Overlay>
          )}

          {/* 7. APP */}
          {geoApp && (
            <LayersControl.Overlay name="Conflito em APP">
              <GeoJSON key={`app-${propertyId}`} data={geoApp} style={{ color: "#8b5cf6", weight: 2, fillColor: "#8b5cf6", fillOpacity: 0.5, smoothFactor: 0 } as any} />
            </LayersControl.Overlay>
          )}

          {/* 8. Assentamentos */}
          {geoSoc && (
            <LayersControl.Overlay name="🏘️ Assentamentos/Comunidades">
              <GeoJSON key={`soc-${propertyId}`} data={geoSoc} style={{ color: "#0ea5e9", weight: 2, fillColor: "#0ea5e9", fillOpacity: 0.3, smoothFactor: 0 } as any} />
            </LayersControl.Overlay>
          )}
        </LayersControl>

        <MapController data={data} />
      </MapContainer>

      <MapLegend />
    </div>
  );
};

export default FarmMap;
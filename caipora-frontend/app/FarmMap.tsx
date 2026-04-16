"use client";

import React, { useMemo } from "react";
import { MapContainer, TileLayer, GeoJSON, LayersControl, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { ExternalLink, ShieldAlert, TreePine, Map as MapIcon, Construction } from "lucide-react";

import { AuditData } from "./caipora/types";
import { ensureIterable } from "./map-utils";
import { MapController } from "./MapController";

interface FarmMapProps {
  data: AuditData;
}

const FarmMap = ({ data }: FarmMapProps) => {
  if (!data || typeof data !== 'object') return (
    <div className="h-full w-full flex items-center justify-center bg-slate-900 text-white rounded-xl min-h-[400px]">
      <div className="animate-pulse flex flex-col items-center gap-4">
        <div className="w-12 h-12 border-4 border-emerald-500 border-t-transparent rounded-full animate-spin"></div>
        <span className="text-sm font-medium tracking-widest uppercase">Carregando Inteligência Geográfica...</span>
      </div>
    </div>
  );

  const propertyId = data?.propertyId ?? "initial";

  // --- ESTILOS TÉCNICOS ---
  const styles = {
    main: { color: "#FFF", weight: 3, fillOpacity: 0.05, dashArray: "5, 5" },
    embargo: { color: "#ef4444", fillColor: "#ef4444", fillOpacity: 0.5, weight: 2 },
    deforestation: { color: "#f97316", fillColor: "#f97316", fillOpacity: 0.6, weight: 1 },
    protected: { color: "#059669", fillColor: "#059669", fillOpacity: 0.4, weight: 1 },
    app: { color: "#3b82f6", fillColor: "#3b82f6", fillOpacity: 0.4, weight: 1 },
    eudr: { color: "#eab308", fillColor: "#eab308", fillOpacity: 0.4, weight: 1 },
    adjacency: { color: "#94a3b8", weight: 2, dashArray: "10, 10", fillOpacity: 0 }
  };

  return (
    <div className="h-full w-full relative rounded-xl overflow-hidden border border-slate-700 shadow-2xl bg-slate-900">
      <MapContainer
        center={[data?.latitude ?? -15, data?.longitude ?? -50]}
        zoom={15}
        scrollWheelZoom={true}
        className="h-full w-full"
        // ADICIONADO: preferCanvas ajuda na captura de polígonos complexos
        preferCanvas={true} 
      >
        <LayersControl position="topright">
          {/* BASES - ADICIONADO crossOrigin="anonymous" EM TODAS AS TILELAYERS */}
          <LayersControl.BaseLayer checked name="Satélite (Híbrido)">
            <TileLayer 
              url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}" 
              maxZoom={22} 
              attribution="Google"
              crossOrigin="anonymous" // CRÍTICO PARA O PDF
            />
          </LayersControl.BaseLayer>
          
          <LayersControl.BaseLayer name="Terreno">
            <TileLayer 
              url="https://mt1.google.com/vt/lyrs=p&x={x}&y={y}&z={z}" 
              maxZoom={22} 
              attribution="Google"
              crossOrigin="anonymous" // CRÍTICO PARA O PDF
            />
          </LayersControl.BaseLayer>

          {/* CAMADA PRINCIPAL: LIMITE DO IMÓVEL */}
          <LayersControl.Overlay checked name="Limite do Imóvel (CAR)">
            <GeoJSON 
              key={`main-${propertyId}`}
              data={ensureIterable(data.geom_car_total || data.geometry)} 
              style={styles.main}
            >
              <Popup>
                <div className="min-w-[200px] p-1">
                  <h3 className="font-bold text-slate-900 border-b pb-1 mb-2">{data.propertyAlias || "Imóvel"}</h3>
                  <p className="text-[10px] text-slate-500 uppercase font-bold">Área Real: {data.area_geometria_ha.toFixed(2)} ha</p>
                </div>
              </Popup>
            </GeoJSON>
          </LayersControl.Overlay>

          {/* CAMADA: EMBARGOS */}
          {data.geom_embargos && (
            <LayersControl.Overlay checked name="Áreas Embargadas">
              <GeoJSON data={ensureIterable(data.geom_embargos)} style={styles.embargo}>
                <Popup><div className="text-red-600 font-bold text-xs">Área com Embargo Ativo</div></Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* CAMADA: DESMATAMENTO */}
          {data.geom_desmatamento && (
            <LayersControl.Overlay checked name="Desmatamento (MapBiomas)">
              <GeoJSON data={ensureIterable(data.geom_desmatamento)} style={styles.deforestation}>
                <Popup><div className="text-orange-600 font-bold text-xs">Supressão Vegetal Detectada</div></Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* CAMADA: ÁREAS PROTEGIDAS (TI, UC, Quilombos) */}
          {data.geom_areas_protegidas && (
            <LayersControl.Overlay checked name="Áreas Protegidas / Sociais">
              <GeoJSON data={ensureIterable(data.geom_areas_protegidas)} style={styles.protected}>
                <Popup><div className="text-emerald-700 font-bold text-xs">Sobreposição Social/Protegida</div></Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* CAMADA: CONFLITO DE APP */}
          {data.geom_conflito_app && (
            <LayersControl.Overlay name="Conflito de APP">
              <GeoJSON data={ensureIterable(data.geom_conflito_app)} style={styles.app}>
                <Popup><div className="text-blue-600 font-bold text-xs">Uso Consolidado em APP</div></Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}

          {/* CAMADA: ADJACÊNCIA DE RISCO */}
          {data.geom_adjacencia_risco && (
            <LayersControl.Overlay name="Vizinhança Crítica">
              <GeoJSON data={ensureIterable(data.geom_adjacencia_risco)} style={styles.adjacency}>
                <Popup><div className="text-slate-500 font-bold text-xs">Elemento de Risco Adjacente</div></Popup>
              </GeoJSON>
            </LayersControl.Overlay>
          )}
        </LayersControl>

        <MapController data={data} />
      </MapContainer>

      {/* LEGENDA FLUTUANTE */}
      <div className="absolute bottom-4 left-4 z-[1000] bg-slate-900/80 backdrop-blur-md p-3 rounded-lg border border-white/10 text-[10px] text-white space-y-2">
        <div className="flex items-center gap-2"><div className="w-3 h-3 bg-red-500 rounded-sm"></div> EMBARGOS</div>
        <div className="flex items-center gap-2"><div className="w-3 h-3 bg-orange-500 rounded-sm"></div> DESMATAMENTO</div>
        <div className="flex items-center gap-2"><div className="w-3 h-3 bg-emerald-500 rounded-sm"></div> ÁREAS PROTEGIDAS</div>
      </div>
    </div>
  );
};

export default FarmMap;
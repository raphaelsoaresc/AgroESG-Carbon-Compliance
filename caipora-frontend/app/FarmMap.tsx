'use client';
import { MapContainer, TileLayer, Polygon, Marker, Popup, useMap } from 'react-leaflet';
import { useEffect } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// 1. Fix para o ícone do Leaflet (essencial para Next.js)
const icon = L.icon({
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41]
});

// 2. Componente para ajustar o enquadramento e forçar o redesenho (evita o bug cinza)
function MapController({ coords, data }: { coords: [number, number][], data: any }) {
  const map = useMap();

  useEffect(() => {
    // Força o Leaflet a recalcular o tamanho do container assim que os dados chegam
    // Isso resolve o problema do mapa carregar "cortado" ou cinza
    setTimeout(() => {
      map.invalidateSize();
    }, 100);

    if (coords && coords.length > 0) {
      const bounds = L.latLngBounds(coords);
      map.fitBounds(bounds, { padding: [50, 50], animate: true });
    }
  }, [coords, map, data]);

  return null;
}

export default function FarmMap({ data }: { data: any }) {
  if (!data || !data.mapCenterCoords) return null;

  // Define a cor do polígono baseada no status
  const polyColor = data.color === 'red' ? '#ef4444' : data.color === 'orange' ? '#f97316' : '#22c55e';

  return (
    <div className="w-full h-full min-h-[350px] relative">
      <MapContainer 
        center={data.mapCenterCoords} 
        zoom={data.mapZoom || 13} 
        style={{ height: '100%', width: '100%', background: '#0f172a' }} // Fundo escuro enquanto carrega
        scrollWheelZoom={true}
      >
        {/* Camada 1: Satélite (Esri World Imagery) */}
        <TileLayer
          url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
          attribution="&copy; Esri"
        />

        {/* Camada 2: Labels Híbridas (Nomes de cidades e estradas transparentes) */}
        <TileLayer
          url="https://stamen-tiles.a.ssl.fastly.net/toner-labels/{z}/{x}/{y}.png"
          opacity={0.7}
        />
        
        {/* Desenha o Polígono da Fazenda */}
        {data.polygonCoords && data.polygonCoords.length > 0 && (
          <>
            <Polygon 
              positions={data.polygonCoords} 
              pathOptions={{ 
                color: polyColor, 
                fillColor: polyColor, 
                fillOpacity: 0.3,
                weight: 3,
                dashArray: data.color === 'red' ? '5, 10' : '0' // Linha tracejada se for crítico
              }} 
            />
            <MapController coords={data.polygonCoords} data={data} />
          </>
        )}

        {/* Marcadores de Alerta (ex: Pontos de Desmatamento ou Sede) */}
        {data.alerts?.map((alert: any, idx: number) => (
          <Marker key={idx} position={alert.coords} icon={icon}>
            <Popup>
              <div className="font-sans">
                <strong className={data.color === 'red' ? 'text-red-600' : 'text-green-600'}>
                  {alert.label}
                </strong>
                <p className="text-xs text-slate-500 mt-1 font-mono">Coordenadas: {alert.coords[0].toFixed(4)}, {alert.coords[1].toFixed(4)}</p>
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>

      {/* Overlay de "Mira" para dar aspecto de software militar/satélite */}
      <div className="absolute inset-0 pointer-events-none border-[20px] border-white/5 z-[400]"></div>
      <div className="absolute top-4 right-4 bg-slate-900/80 backdrop-blur-md text-[10px] text-white px-2 py-1 rounded border border-white/10 z-[400] font-mono">
        LAT: {data.mapCenterCoords[0].toFixed(4)} | LNG: {data.mapCenterCoords[1].toFixed(4)}
      </div>
    </div>
  );
}
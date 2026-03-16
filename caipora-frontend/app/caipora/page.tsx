'use client';
import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import Image from 'next/image';
import Link from 'next/link';

const FarmMap = dynamic(() => import('../FarmMap'), { // Mudou de ./ para ../
  ssr: false,
  loading: () => <div className="flex items-center justify-center h-full bg-slate-800 text-slate-400 font-mono text-sm">📡 Conectando ao Satélite...</div>
});

// IDs de Demonstração (Acesso Total e Ilimitado)
const DEMO_IDS = [
  'MT-5107859-9DFDE64A2FFC4556B116F9BDE0C6595F',
  'AM-1303569-85EECD549EC34411BEBF5142E59E304A',
  'PA-1503754-5E969C33E8D14256A06C6452F71A113D'
];

const parseGeometry = (geom: any) => {
  if (!geom) return [];
  if (typeof geom === 'object' && geom.type && geom.coordinates) {
    try {
      if (geom.type === 'Polygon') return geom.coordinates[0].map((pt: any) => [pt[1], pt[0]]);
      if (geom.type === 'MultiPolygon') return geom.coordinates[0][0].map((pt: any) => [pt[1], pt[0]]);
    } catch (e) { return []; }
  }
  return [];
};

export default function Home() {
  const [carId, setCarId] = useState('');
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [searchCount, setSearchCount] = useState(0);

  useEffect(() => {
    const savedCount = localStorage.getItem('caipora_search_count');
    if (savedCount) setSearchCount(parseInt(savedCount));
  }, []);

  const formatCurrency = (value: number) => 
    new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(value);

  const performSearch = async (idToSearch: string) => {
    if (!idToSearch) return;

    const isDemo = DEMO_IDS.includes(idToSearch);

    if (!isDemo && searchCount >= 3) {
      alert("🔒 Limite de 3 consultas atingido. Entre em contato para liberar o acesso total.");
      return;
    }

    setLoading(true);
    setCarId(idToSearch);

    try {
      const response = await fetch(`/api/compliance/${idToSearch}`);
      const record = await response.json();

      if (!record || record.error || !record.property_id) {
        alert("Imóvel não encontrado.");
        setLoading(false);
        return;
      }

      if (!isDemo) {
        const newCount = searchCount + 1;
        setSearchCount(newCount);
        localStorage.setItem('caipora_search_count', newCount.toString());
      }

      const rawEvidence = record.risk_analysis?.technical_evidence || '';
      const cleanEvidenceList = rawEvidence
        .replace(/\[.*?\]/g, '') 
        .split('|')
        .map((s: string) => s.trim())
        .filter((s: string) => s.length > 5 && !s.includes('Vizinho com'));

      const statusString = record.verdict || 'ANALYSING';
      const colorTheme = statusString.includes('NOT ELIGIBLE') ? 'red' : statusString.includes('WARNING') ? 'orange' : 'green';

      setData({
        status: statusString,
        liabilityTotal: formatCurrency(record.financial_liabilities?.estimated_total_brl || 0),
        liabilityDeforestation: formatCurrency(record.financial_liabilities?.deforestation_liability || 0),
        liabilitySocial: formatCurrency(record.financial_liabilities?.social_liability || 0),
        uf: record.uf,
        city: record.city,
        area: record.total_area_ha,
        protectedOverlap: record.protected_area_overlap_ha,
        mapbiomasAlertId: record.deforestation_metrics?.mapbiomas_alert_id,
        evidenceList: cleanEvidenceList,
        metrics: `Bioma: ${record.environmental_score?.biome} | NDVI: ${Number(record.environmental_score?.general_ndvi_mean).toFixed(2)} | Declividade: ${Number(record.max_slope_degrees).toFixed(1)}°`,
        color: colorTheme,
        mapCenterCoords: parseGeometry(record.geometry)[0] || [-15, -55],
        polygonCoords: parseGeometry(record.geometry),
        isCensored: !isDemo 
      });

    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex flex-col">
      
      {/* HEADER */}
<header className="bg-gradient-to-br from-slate-900 via-green-950 to-slate-900 pb-32 pt-16 px-6 text-white shadow-2xl border-b-4 border-green-500">
  <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center gap-12 mb-16">
    
    {/* AQUI ESTÁ A MUDANÇA: Envolvi a div com o componente <Link> */}
    <Link 
      href="/" 
      className="w-40 h-40 md:w-48 md:h-48 relative rounded-[2.5rem] overflow-hidden bg-white shadow-[0_0_60px_rgba(34,197,94,0.4)] border-4 border-white/20 shrink-0 block hover:scale-105 transition-transform cursor-pointer"
    >
      <Image 
        src="/logo-caipora.jpg" 
        alt="Caipora Sentinela" 
        fill 
        className="object-cover" 
      />
    </Link>

    <div className="flex-1 space-y-4 text-center md:text-left">
      <span className="text-5xl md:text-7xl font-black tracking-tighter block">
        <span className="text-green-400">Caipora</span> Sentinela
      </span>
      <span className="text-slate-400 text-xl font-bold tracking-[0.5em] uppercase">
        Compliance Geoespacial
      </span>
    </div>
  </div>

        <div className="max-w-4xl mx-auto">
          <div className="relative bg-white p-2 rounded-[2rem] shadow-2xl flex flex-col md:flex-row gap-2">
            <input 
              type="text" value={carId} onChange={(e) => setCarId(e.target.value)}
              placeholder="Digite o código do CAR..."
              className="flex-1 p-6 rounded-2xl text-slate-900 text-xl font-mono outline-none"
            />
            <button onClick={() => performSearch(carId)} className="bg-slate-900 hover:bg-black text-white px-12 py-6 rounded-2xl font-black text-xl transition-all active:scale-95">
              EXECUTAR AUDITORIA
            </button>
            <div className="absolute -top-10 right-4 bg-white/10 backdrop-blur-md border border-white/20 px-4 py-1 rounded-full text-[10px] font-bold uppercase tracking-widest">
              Consultas Restantes: <span className={searchCount >= 3 ? "text-red-400" : "text-green-400"}>{3 - searchCount}</span>
            </div>
          </div>
          
          <div className="flex justify-center gap-4 mt-8">
            <button onClick={() => performSearch('MT-5107859-9DFDE64A2FFC4556B116F9BDE0C6595F')} className="group flex items-center gap-2 text-xs bg-red-500/10 hover:bg-red-500/20 text-red-200 px-6 py-3 rounded-full border border-red-500/30 transition-all backdrop-blur-sm">
              <span>🔥</span> Demo Risco Crítico
            </button>
            <button onClick={() => performSearch('AM-1303569-85EECD549EC34411BEBF5142E59E304A')} className="group flex items-center gap-2 text-xs bg-orange-500/10 hover:bg-orange-500/20 text-orange-200 px-6 py-3 rounded-full border border-orange-500/30 transition-all backdrop-blur-sm">
              <span>⚠️</span> Demo Alerta
            </button>
            <button onClick={() => performSearch('PA-1503754-5E969C33E8D14256A06C6452F71A113D')} className="group flex items-center gap-2 text-xs bg-green-500/10 hover:bg-green-500/20 text-green-200 px-6 py-3 rounded-full border border-green-500/30 transition-all backdrop-blur-sm">
              <span>✅</span> Demo Conformidade
            </button>
          </div>
        </div>
      </header>

      {/* CONTEÚDO PRINCIPAL */}
      <main className="max-w-6xl mx-auto px-6 -mt-16 pb-20 relative z-10 flex-grow w-full">
        {data && (
          <section className="grid grid-cols-1 lg:grid-cols-3 gap-8 animate-in fade-in slide-in-from-bottom-10 duration-700">
            <div className="lg:col-span-2 space-y-8">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                
                {/* VERDITO */}
                <div className={`bg-white p-8 rounded-[2rem] shadow-xl border-t-[12px] ${data.color === 'red' ? 'border-red-500' : data.color === 'orange' ? 'border-orange-500' : 'border-green-500'}`}>
                  <p className="text-slate-400 text-xs font-black uppercase tracking-widest mb-4">Veredito Final</p>
                  <h3 className={`text-4xl font-black leading-none ${data.color === 'red' ? 'text-red-600' : data.color === 'orange' ? 'text-orange-500' : 'text-green-600'}`}>
                    {data.status}
                  </h3>
                  <div className="mt-6 flex items-center gap-2 text-slate-600 font-bold">
                    <span className="bg-slate-100 px-3 py-1 rounded-lg text-sm">{data.city} - {data.uf}</span>
                    <span className="bg-slate-100 px-3 py-1 rounded-lg text-sm">{data.area.toFixed(2)} ha</span>
                  </div>
                </div>

                {/* PASSIVO FINANCEIRO (AJUSTADO TAMANHO DA FONTE) */}
                <div className="bg-slate-900 p-8 rounded-[2rem] shadow-xl border-t-[12px] border-emerald-400 text-white relative overflow-hidden">
                  <p className="text-emerald-400 text-xs font-black uppercase tracking-widest mb-4">Passivo Financeiro Total</p>
                  {data.isCensored ? (
                    <div className="space-y-2">
                      <h3 className="text-4xl font-black tracking-tighter blur-md select-none">R$ 9.999.999</h3>
                      <p className="text-[10px] text-emerald-500 font-bold bg-emerald-500/10 p-2 rounded border border-emerald-500/20">🔒 CONTRATE O PLANO PRO PARA VER VALORES</p>
                    </div>
                  ) : (
                    <>
                      <h3 className="text-4xl font-black tracking-tighter mb-6 break-words leading-tight">
                        {data.liabilityTotal}
                      </h3>
                      <div className="space-y-2 border-t border-white/10 pt-4">
                        <div className="flex justify-between text-[10px] uppercase font-bold tracking-widest">
                          <span className="text-slate-400">Ambiental:</span>
                          <span className="text-emerald-400">{data.liabilityDeforestation}</span>
                        </div>
                        <div className="flex justify-between text-[10px] uppercase font-bold tracking-widest">
                          <span className="text-slate-400">Social:</span>
                          <span className="text-emerald-400">{data.liabilitySocial}</span>
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </div>

              {/* EVIDÊNCIAS (COM LINK MAPBIOMAS) */}
              <div className="bg-white rounded-[2rem] shadow-xl border border-slate-200 overflow-hidden relative">
                <div className="bg-slate-50 border-b p-6 flex justify-between items-center">
                  <h4 className="font-black text-slate-700 text-sm uppercase tracking-widest">Evidências de Auditoria</h4>
                  {!data.isCensored && <span className="text-xs font-mono text-slate-400 bg-white px-4 py-1 rounded-full border border-slate-200">{data.metrics}</span>}
                </div>
                
                <div className={`p-8 space-y-4 ${data.isCensored ? 'blur-sm grayscale pointer-events-none select-none' : ''}`}>
                  {data.evidenceList.map((text: string, index: number) => {
                    const isMapBiomas = text.includes('MapBiomas');
                    const isCrit = text.includes('Violação') || text.includes('RESTRIÇÃO') || text.includes('Sobreposição');

                    return (
                      <div key={index} className={`flex flex-col gap-3 p-5 rounded-2xl border transition-all ${isCrit ? 'bg-red-50 border-red-100 text-red-900' : 'bg-slate-50 border-slate-100 text-slate-700'}`}>
                        <div className="flex items-center gap-6">
                          <div className={`h-4 w-4 rounded-full shrink-0 ${isCrit ? 'bg-red-500 animate-pulse' : 'bg-slate-300'}`} />
                          <p className="text-lg font-mono leading-tight flex-1">{text}</p>
                        </div>
                        
                        {/* LINK DINÂMICO MAPBIOMAS */}
                        {isMapBiomas && data.mapbiomasAlertId && (
                          <div className="ml-10">
                            <a 
                              href={`https://plataforma.alerta.mapbiomas.org/alerta/${data.mapbiomasAlertId}`}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center gap-2 bg-white border border-red-200 text-red-600 px-4 py-2 rounded-xl text-xs font-black hover:bg-red-600 hover:text-white transition-all shadow-sm"
                            >
                              🔗 ACESSAR LAUDO MAPBIOMAS {data.mapbiomasAlertId}
                            </a>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>

                {data.isCensored && (
                  <div className="absolute inset-0 flex items-center justify-center bg-white/40 backdrop-blur-[2px] z-20">
                    <div className="bg-slate-900 text-white p-6 rounded-2xl shadow-2xl text-center space-y-3 border border-white/20">
                      <span className="text-3xl">🔒</span>
                      <h5 className="font-black uppercase tracking-widest text-sm">Relatório Detalhado Bloqueado</h5>
                      <p className="text-xs text-slate-400 max-w-[200px]">As evidências bitemporais estão disponíveis apenas na versão completa.</p>
                      <a 
                        href="mailto:compliance@agrimarketintel.com?subject=Solicitação de Acesso Pro - Caipora Sentinela"
                        className="w-full bg-green-500 text-slate-900 font-black py-3 rounded-lg text-[10px] uppercase tracking-tighter text-center block hover:bg-green-400 transition-colors"
                      >
                        Falar com Especialista
                      </a>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* MAPA */}
            <div className="bg-slate-900 rounded-[2rem] shadow-2xl border border-slate-800 overflow-hidden min-h-[500px] flex flex-col relative">
              <div className="bg-slate-950 p-6 border-b border-slate-800 flex justify-between items-center">
                <span className="text-white text-xs font-black uppercase tracking-widest flex items-center gap-3">
                  <span className="h-3 w-3 bg-green-500 rounded-full animate-pulse"></span> Monitoramento Ativo
                </span>
              </div>
              
              <div className={`flex-1 relative ${data.isCensored ? 'blur-xl grayscale brightness-50' : ''}`}>
                <FarmMap key={data.city} data={data} />
              </div>

              {data.isCensored && (
                <div className="absolute inset-0 flex items-center justify-center z-30">
                  <div className="text-center space-y-4">
                    <div className="bg-white/10 backdrop-blur-md p-6 rounded-full inline-block border border-white/20">
                      <span className="text-4xl">🛰️</span>
                    </div>
                    <h5 className="text-white font-black uppercase tracking-[0.3em] text-xs">Imagens de Satélite Ocultas</h5>
                  </div>
                </div>
              )}
            </div>
          </section>
        )}
      </main>

      {/* RODAPÉ MESTRE - UNIFICADO (COM O TEU SLOGAN) */}
      <footer className="bg-white border-t border-slate-200 pt-24 pb-12 px-6 mt-20">
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-16 mb-16">
            
            {/* Coluna 1: Logo Agri-Market (Tamanho Grande) */}
            <div className="space-y-8">
              <div className="relative w-72 h-32 md:w-[450px] md:h-48">
                <Image 
                  src="/logo-agrimarket.jpg" 
                  alt="Agri-Market Intelligence & Risk Automation" 
                  fill 
                  className="object-contain object-left"
                />
              </div>
              {/* O TEU SLOGAN ABAIXO */}
              <p className="text-slate-500 text-xl leading-relaxed font-medium max-w-sm">
                Dados que plantam, tecnologia que protege.
              </p>
            </div>

            {/* Coluna 2: Contato Oficial */}
            <div className="space-y-6">
              <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Contato Oficial</h5>
              <ul className="space-y-4 text-base text-slate-600">
                <li className="flex items-center gap-3">
                  <span className="bg-slate-100 p-2 rounded-lg text-green-600 font-bold">✉</span>
                  <a href="mailto:compliance@agrimarketintel.com" className="hover:text-green-600 transition-colors font-semibold">
                    compliance@agrimarketintel.com
                  </a>
                </li>
                <li className="flex items-center gap-3">
                  <span className="bg-slate-100 p-2 rounded-lg text-green-600 font-bold">🌐</span>
                  <a href="https://agrimarketintel.com" target="_blank" rel="noopener noreferrer" className="hover:text-green-600 transition-colors font-semibold">
                    www.agrimarketintel.com
                  </a>
                </li>
              </ul>
            </div>

            {/* Coluna 3: Data Sources */}
            <div className="space-y-6">
              <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Data Sources</h5>
              <div className="flex flex-wrap gap-2">
                {['IBAMA', 'INCRA', 'MAPBIOMAS', 'INPE', 'MMA', 'EUDR-READY', 'CMN-5081'].map((source) => (
                  <span key={source} className="bg-slate-900 text-white text-[10px] font-black px-3 py-1.5 rounded-md tracking-widest">
                    {source}
                  </span>
                ))}
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed italic font-medium mt-4">
                As análises geradas pela plataforma utilizam dados públicos e algoritmos proprietários de Risk Automation.
              </p>
            </div>
          </div>

          {/* Barra Inferior: Copyright, Jurídico e API */}
          <div className="border-t border-slate-100 pt-10 flex flex-col md:flex-row justify-between items-center gap-8">
            <p className="text-sm text-slate-400 font-bold">
              © {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation.
            </p>
            <div className="flex flex-wrap justify-center gap-8 text-xs font-black uppercase tracking-widest text-slate-400">
              <Link href="/termos" className="hover:text-slate-900 transition-colors">Termos</Link>
              <Link href="/privacidade" className="hover:text-slate-900 transition-colors">Privacidade</Link>
              <a 
                href="https://caipora-sentinela-api-534128993934.us-central1.run.app/docs" 
                target="_blank" 
                rel="noopener noreferrer" 
                className="hover:text-slate-900 transition-colors border-b-2 border-green-500/30 pb-1"
              >
                API OAS 3.1 (Swagger)
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}
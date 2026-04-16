'use client';

import { useEffect, useState, useCallback, useRef } from 'react';
import dynamic from 'next/dynamic';
import { Lock, Activity, FileDown, RefreshCw, CheckCircle2, Mail } from 'lucide-react';
import { PDFDownloadLink } from '@react-pdf/renderer';
import { toPng } from 'html-to-image';

import LeadForm from './LeadForm';
import Header from './Header';
import Footer from './Footer';
import { useAudit } from './hooks/useAudit';
import { SummaryCard } from './components/SummaryCard';
import { FinancialCard } from './components/FinancialCard';
import { EvidenceSection } from './components/EvidenceSection';
import AuditReportTemplate from './components/AuditReportTemplate';

// Importação dinâmica do Mapa
const FarmMap = dynamic(() => import('../FarmMap'), {
  ssr: false,
  loading: () => (
    <div className="flex flex-col items-center justify-center h-full bg-slate-900 text-slate-500 gap-3">
      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-emerald-500"></div>
      <span className="text-xs font-medium tracking-widest uppercase">Sincronizando Satélite...</span>
    </div>
  )
});

export default function CaiporaPage() {
  const { 
    isAdmin, 
    logout, 
    carId, 
    setCarId, 
    performSearch, 
    searchCount,
    data,
    loading,
    limitReached,
    handleUnlockReport 
  } = useAudit();

  // ESTADOS PARA CAPTURA DO MAPA
  const [mapImage, setMapImage] = useState<string | null>(null);
  const [isCapturing, setIsCapturing] = useState(false);
  const [lastCapturedId, setLastCapturedId] = useState<string | null>(null);
  
  // Referência para evitar múltiplas capturas simultâneas
  const isProcessing = useRef(false);
  const lastCapturedIdRef = useRef<string | null>(null);

  /**
   * Função para capturar a área do mapa.
   */
  const captureMapImage = useCallback(async () => {
    if (isProcessing.current) return;
    
    const mapElement = document.getElementById('map-viewport');
    if (!mapElement || !data) return;

    try {
      isProcessing.current = true;
      setIsCapturing(true);
      
      await new Promise(resolve => setTimeout(resolve, 4000));
      
      const dataUrl = await toPng(mapElement, { 
        canvasWidth: 1000,
        canvasHeight: 1000,
        pixelRatio: 1.5, 
        cacheBust: true,
        backgroundColor: '#0f172a',
        filter: (node) => {
          const className = (node as HTMLElement).className;
          if (typeof className === 'string') {
            return !className.includes('leaflet-control');
          }
          return true;
        }
      });
      
      setMapImage(dataUrl);
      lastCapturedIdRef.current = data.propertyId;
      setLastCapturedId(data.propertyId);
    } catch (err) {
      console.error('Falha ao capturar imagem do satélite:', err);
    } finally {
      setIsCapturing(false);
      isProcessing.current = false;
    }
  }, [data]);

  // Efeito para disparar a busca caso o ID venha pela URL
  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const carFromUrl = urlParams.get('car');
    if (carFromUrl) {
      performSearch(carFromUrl);
    }
  }, [performSearch]);

  // Efeito para capturar o mapa automaticamente
  useEffect(() => {
    if (data && !loading && data.propertyId !== lastCapturedIdRef.current) {
      setMapImage(null); 
      captureMapImage();
    }
  }, [data, loading, captureMapImage]);

  // Helper para gerar o link de e-mail
  const getMailtoLink = (id: string) => {
    const subject = encodeURIComponent(`Solicitação de Desbloqueio: ${id}`);
    const body = encodeURIComponent(`Olá, gostaria de solicitar o desbloqueio do relatório completo para o imóvel ${id}.`);
    return `mailto:compliance@agrimarketintel.com?subject=${subject}&body=${body}`;
  };

  return (
    <div className="min-h-screen bg-[#F8FAFC] flex flex-col">
      
      <Header 
        carId={carId} 
        setCarId={setCarId} 
        onSearch={performSearch} 
        searchCount={searchCount}
        isAdmin={isAdmin}
        logout={logout}
      />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 -mt-20 pb-20 relative z-10 flex-grow w-full">
        
        {/* Paywall */}
        {!isAdmin && limitReached && !data && !loading && (
          <div className="bg-white rounded-[2.5rem] shadow-2xl border border-slate-200 p-12 text-center space-y-8 animate-in fade-in zoom-in duration-500">
            <div className="inline-flex items-center justify-center bg-amber-50 p-6 rounded-full text-amber-600 mb-4">
              <Lock className="w-12 h-12" />
            </div>
            <h2 className="text-4xl font-bold text-slate-900 tracking-tight">Limite de Consultas Atingido</h2>
            <p className="text-slate-500 text-lg max-w-2xl mx-auto">
              Você utilizou suas consultas gratuitas. Para auditar o imóvel <span className="font-mono font-bold text-slate-900">{carId}</span>, fale com um de nossos especialistas:
            </p>
            <div className="max-w-md mx-auto pt-4">
              <a 
                href={getMailtoLink(carId)}
                className="w-full inline-flex items-center justify-center gap-3 bg-slate-900 hover:bg-slate-800 text-white px-8 py-5 rounded-2xl text-lg font-bold transition-all shadow-xl active:scale-95"
              >
                <Mail className="w-6 h-6" />
                Contatar Compliance
              </a>
            </div>
          </div>
        )}

        {loading && (
          <div className="bg-white/80 backdrop-blur-md p-12 rounded-3xl shadow-sm border border-slate-200 text-center animate-pulse mb-8">
            <p className="text-slate-500 font-semibold uppercase tracking-widest text-sm">Processando Inteligência Geoespacial...</p>
          </div>
        )}

        {/* Dashboard */}
        {(data || isAdmin) && !loading && (
          <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
            <section className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
              
              <div className="lg:col-span-7 space-y-6">
                {data && (
                  <>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <SummaryCard data={data} />
                      <FinancialCard data={data} />
                    </div>
                    <EvidenceSection data={data} onUnlock={handleUnlockReport} carId={carId} />
                  </>
                )}
              </div>

              <div className="lg:col-span-5 sticky top-24">
                <div className="bg-white p-3 rounded-[2rem] shadow-xl border border-slate-200 relative">
                  <div id="map-viewport" className="relative w-full aspect-square rounded-[1.5rem] overflow-hidden bg-slate-900 border border-slate-800 shadow-inner">
                    <div className="absolute top-0 left-0 right-0 z-[1000] bg-slate-950/40 backdrop-blur-md p-4 border-b border-white/10 flex justify-between items-center">
                      <span className="text-white text-[10px] font-bold uppercase tracking-[0.2em] flex items-center gap-2">
                        <Activity className="w-3 h-3 text-emerald-500 animate-pulse" /> Monitoramento Satelital
                      </span>
                      {data && (
                        <button 
                          onClick={() => { lastCapturedIdRef.current = null; captureMapImage(); }}
                          disabled={isCapturing}
                          className="bg-white/10 hover:bg-white/20 p-2 rounded-full text-white transition-all"
                        >
                          <RefreshCw className={`w-3.5 h-3.5 ${isCapturing ? 'animate-spin' : ''}`} />
                        </button>
                      )}
                    </div>
                    <div className="h-full w-full">
                      {data && <FarmMap data={data} />}
                    </div>
                  </div>
                  {isCapturing && (
                    <div className="absolute inset-3 rounded-[1.5rem] bg-slate-900/60 backdrop-blur-sm z-[2000] flex items-center justify-center">
                      <div className="text-center">
                        <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-white mx-auto mb-2"></div>
                        <p className="text-white text-[10px] font-bold uppercase tracking-widest">Sincronizando Foto...</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </section>

            {/* Ações do Relatório */}
            {data && (
              <div className="flex flex-col sm:flex-row justify-between items-center bg-white p-6 rounded-3xl border border-slate-200 shadow-sm gap-4">
                <div className="flex items-center gap-4">
                  <div className={`flex items-center justify-center w-12 h-12 rounded-2xl ${mapImage ? 'bg-emerald-50 text-emerald-600' : 'bg-slate-100 text-slate-400'}`}>
                    {mapImage ? <CheckCircle2 className="w-6 h-6" /> : <Activity className="w-6 h-6 animate-pulse" />}
                  </div>
                  <div>
                    <p className="text-xs font-bold text-slate-400 uppercase tracking-wider">Status do Relatório</p>
                    <p className="text-sm font-bold text-slate-900">
                      {data.isCensored ? 'Relatório Bloqueado para Exportação' : (mapImage ? 'Documento Técnico Consolidado' : 'Sincronizando Imagem de Satélite...')}
                    </p>
                  </div>
                </div>
                
                {data.isCensored ? (
                  <a 
                    href={getMailtoLink(data.propertyId)}
                    className="w-full sm:w-auto inline-flex items-center justify-center gap-2 bg-amber-600 hover:bg-amber-700 text-white px-8 py-4 rounded-2xl text-sm font-bold transition-all shadow-xl shadow-amber-100 active:scale-95"
                  >
                    <Lock className="w-5 h-5" />
                    Solicitar Desbloqueio via E-mail
                  </a>
                ) : !mapImage || isCapturing ? (
                  <button className="w-full sm:w-auto inline-flex items-center justify-center gap-2 bg-slate-100 text-slate-400 px-8 py-4 rounded-2xl text-sm font-bold cursor-not-allowed">
                    <FileDown className="w-5 h-5 animate-bounce" />
                    Preparando Documento...
                  </button>
                ) : (
                  <PDFDownloadLink
                    key={mapImage}
                    document={<AuditReportTemplate data={{ ...data, map_image_satellite: mapImage } as any} />}
                    fileName={`Auditoria_Caipora_${data.propertyId}.pdf`}
                    className="w-full sm:w-auto inline-flex items-center justify-center gap-2 bg-slate-900 hover:bg-slate-800 text-white px-8 py-4 rounded-2xl text-sm font-bold transition-all shadow-xl shadow-slate-200 active:scale-95"
                  >
                    {({ loading: pdfLoading }) => (
                      <>
                        <FileDown className={`w-5 h-5 ${pdfLoading ? 'animate-bounce' : ''}`} />
                        {pdfLoading ? 'Gerando PDF...' : 'Exportar Auditoria Completa (PDF)'}
                      </>
                    )}
                  </PDFDownloadLink>
                )}
              </div>
            )}
          </div>
        )}
      </main>
      <Footer />
    </div>
  );
}
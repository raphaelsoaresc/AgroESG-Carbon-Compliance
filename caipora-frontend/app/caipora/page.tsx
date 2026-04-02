'use client';

import { useEffect } from 'react';
import dynamic from 'next/dynamic';
import { Lock, ArrowRight, Activity } from 'lucide-react';
import LeadForm from './LeadForm';
import Header from './Header';
import Footer from './Footer';
import PaymentBrick from './PaymentBrick';
import { useAudit } from './hooks/useAudit';
import { SummaryCard } from './components/SummaryCard';
import { FinancialCard } from './components/FinancialCard';
import { EvidenceSection } from './components/EvidenceSection';

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
    showPayment,
    preferenceId,
    handleUnlockReport
  } = useAudit();

  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const carFromUrl = urlParams.get('car');
    if (carFromUrl) {
      performSearch(carFromUrl);
    }
  }, []);

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
        
        {/* Se NÃO for admin e atingir o limite, mostra o Paywall */}
        {!isAdmin && limitReached && !data && !loading && (
          <div className="bg-white rounded-[2.5rem] shadow-2xl border border-slate-200 p-12 text-center space-y-8 animate-in fade-in zoom-in duration-500">
            <div className="inline-flex items-center justify-center bg-red-50 p-6 rounded-full text-red-500 mb-4">
              <Lock className="w-12 h-12" />
            </div>
            <h2 className="text-4xl font-bold text-slate-900 tracking-tight">Limite de Consultas Atingido</h2>
            <p className="text-slate-500 text-lg max-w-2xl mx-auto">
              Você utilizou suas 3 consultas gratuitas. Para auditar o imóvel <span className="font-mono font-bold text-slate-900">{carId}</span>:
            </p>
            <div className="max-w-md mx-auto space-y-6 pt-4">
              {showPayment && preferenceId ? (
                <div className="animate-in fade-in duration-300">
                  <PaymentBrick
                    preferenceId={preferenceId}
                    onPaymentSuccess={() => { performSearch(carId); }}
                  />
                </div>
              ) : (
                <>
                  <button onClick={handleUnlockReport} className="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-5 rounded-2xl shadow-xl shadow-emerald-100 transition-all active:scale-95 text-lg flex items-center justify-center gap-3">
                    LIBERAR ESTE LAUDO (R$ 150) <ArrowRight className="w-5 h-5" />
                  </button>
                  <div className="relative flex items-center justify-center">
                    <div className="absolute inset-0 flex items-center"><span className="w-full border-t border-slate-200"></span></div>
                    <span className="relative bg-white px-4 text-[10px] font-bold uppercase tracking-widest text-slate-400">Ou fale com um especialista</span>
                  </div>
                  <LeadForm carId={carId} />
                </>
              )}
            </div>
          </div>
        )}

        {loading && (
          <div className="bg-white/80 backdrop-blur-md p-12 rounded-3xl shadow-sm border border-slate-200 text-center animate-pulse mb-8">
            <p className="text-slate-500 font-semibold uppercase tracking-widest text-sm">Processando Inteligência Geoespacial...</p>
          </div>
        )}

        {/* Se for admin OU se tiver dados, mostra o Dashboard */}
        {(data || isAdmin) && !loading && (
          <section className="grid grid-cols-1 lg:grid-cols-12 gap-6 animate-in fade-in slide-in-from-bottom-4 duration-700">
            <div className="lg:col-span-8 space-y-6">
              {/* Só renderiza os cards se houver data (evita erro de null no TS) */}
              {data && (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <SummaryCard data={data} />
                    <FinancialCard data={data} />
                  </div>
                  <EvidenceSection data={data} onUnlock={handleUnlockReport} carId={carId} />
                </>
              )}
              {!data && isAdmin && (
                <div className="bg-white p-12 rounded-3xl border border-dashed border-slate-300 text-center text-slate-400">
                  Aguardando consulta de CAR para exibir indicadores...
                </div>
              )}
            </div>

            <div className="lg:col-span-4 h-[600px] lg:h-auto sticky top-24">
              <div className="bg-slate-900 rounded-3xl shadow-2xl border border-slate-800 overflow-hidden h-full flex flex-col relative">
                <div className="bg-slate-950/50 backdrop-blur-md p-5 border-b border-slate-800 flex justify-between items-center">
                  <span className="text-white text-[10px] font-bold uppercase tracking-[0.2em] flex items-center gap-2">
                    <Activity className="w-3 h-3 text-emerald-500 animate-pulse" /> Monitoramento Satelital
                  </span>
                </div>
                <div className="flex-1 relative">
                  {data && <FarmMap key={data.carNumber} data={data} />}
                </div>
              </div>
            </div>
          </section>
        )}
      </main>
      <Footer />
    </div>
  );
}
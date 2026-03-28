'use client';

import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import Link from 'next/link'; // <-- Importação do Link adicionada aqui
import LeadForm from './LeadForm';
import Header from './Header';
import Footer from './Footer';
import PaymentBrick from './PaymentBrick';
import { AuditData, DEMO_IDS } from './types';
import { parseGeometry, formatCurrency, cleanEvidence } from './utils';

// Importação dinâmica do Mapa para evitar erros de SSR
const FarmMap = dynamic(() => import('../FarmMap'), {
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full bg-slate-800 text-slate-400 font-mono text-sm">
      📡 Conectando ao Satélite...
    </div>
  )
});

export default function CaiporaPage() {
  // Estados de Dados e Busca
  const [carId, setCarId] = useState('');
  const [data, setData] = useState<AuditData | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchCount, setSearchCount] = useState(0);

  // Estados de Pagamento e Bloqueio
  const [showPayment, setShowPayment] = useState(false);
  const[preferenceId, setPreferenceId] = useState<string | null>(null);
  const[limitReached, setLimitReached] = useState(false);

  // Carrega o contador de buscas ao iniciar
  useEffect(() => {
    const savedCount = localStorage.getItem('caipora_search_count');
    if (savedCount) {
      const count = parseInt(savedCount);
      setSearchCount(count);
      if (count >= 3) setLimitReached(true);
    }
  },[]);

  /**
   * Função para gerar a preferência de pagamento no Mercado Pago
   */
  const handleUnlockReport = async () => {
    // Limpa a URL removendo qualquer barra final /
    const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "").replace(/\/$/, "");
    const apiKey = process.env.NEXT_PUBLIC_API_KEY;

    console.log("Chamando API em:", `${apiUrl}/payments/create-preference`);

    setLoading(true);
    try {
      const response = await fetch(`${apiUrl}/payments/create-preference`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": apiKey || ""
        },
        body: JSON.stringify({
          car_id: carId,
          email: "compliance@agrimarketintel.com"
        })
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Erro ${response.status}: ${errorText}`);
      }

      const result = await response.json();

      // Extração segura do ID
      let prefId = result.id || result.preference_id;
      if (!prefId && result.init_point) {
        const url = new URL(result.init_point);
        prefId = url.searchParams.get('pref_id');
      }

      if (prefId) {
        setPreferenceId(prefId);
        setShowPayment(true);
      } else {
        alert("A API não retornou um ID de pagamento válido.");
      }
    } catch (error: any) {
      console.error("Erro de Rede:", error);
      alert(`❌ Falha de conexão: Verifique se a URL no .env.local está correta e se o Cloud Run está público.`);
    } finally {
      setLoading(false);
    }
  };

  /**
   * Executa a busca de compliance
   */
  const performSearch = async (idToSearch: string) => {
    if (!idToSearch) return;
    const isDemo = DEMO_IDS.includes(idToSearch);

    if (!isDemo && searchCount >= 3) {
      setLimitReached(true);
      setCarId(idToSearch);
      setData(null);
      return;
    }

    setLoading(true);
    setCarId(idToSearch);
    setShowPayment(false);
    setPreferenceId(null); // Garante que o Brick antigo morra antes da nova busca
    setLimitReached(false);

    // Pega as configurações do ambiente (igual você fez no handleUnlockReport)
    const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    const apiKey = process.env.NEXT_PUBLIC_API_KEY;

    try {
      // CORREÇÃO: URL correta (/compliance/car/) e inclusão dos HEADERS com a API KEY
      const response = await fetch(`${apiUrl}/compliance/car/${idToSearch}`, {
        method: "GET",
        headers: {
          "X-API-Key": apiKey || "",
          "Content-Type": "application/json"
        }
      });

      if (!response.ok) {
        // Se o backend retornar 404, 403 ou 500, cai aqui
        alert(`Imóvel não encontrado ou erro na API (Status: ${response.status})`);
        setLoading(false);
        return;
      }

      const record = await response.json();

      if (!record || record.error) {
        alert("Imóvel não encontrado.");
        setLoading(false);
        return;
      }

      // ... resto do código (incremento de contador e setData) permanece igual
      if (!isDemo) {
        const newCount = searchCount + 1;
        setSearchCount(newCount);
        localStorage.setItem('caipora_search_count', newCount.toString());
        if (newCount >= 3) setLimitReached(true);
      }

      const statusString = record.verdict || 'ANALYSING';

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
        evidenceList: cleanEvidence(record.risk_analysis?.technical_evidence || ''),
        metrics: `Bioma: ${record.environmental_score?.biome} | NDVI: ${Number(record.environmental_score?.general_ndvi_mean).toFixed(2)}`,
        color: statusString.includes('NOT ELIGIBLE') ? 'red' : statusString.includes('WARNING') ? 'orange' : 'green',
        mapCenterCoords: parseGeometry(record.geometry)[0] || [-15, -55],
        polygonCoords: parseGeometry(record.geometry),
        isCensored: !isDemo
      });
    } catch (error) {
      console.error("Erro na busca:", error);
      alert("Erro de conexão com o servidor.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex flex-col">
      <Header carId={carId} setCarId={setCarId} onSearch={performSearch} searchCount={searchCount} />

      <main className="max-w-6xl mx-auto px-6 -mt-16 pb-20 relative z-10 flex-grow w-full">
        
        {/* TELA DE LIMITE ATINGIDO (BLOQUEIO TOTAL) */}
        {limitReached && !data && !loading && (
          <div className="bg-white rounded-[3rem] shadow-2xl border-2 border-red-100 p-12 text-center space-y-8 animate-in fade-in zoom-in duration-500">
            <div className="inline-block bg-red-50 p-6 rounded-full text-5xl mb-4">🔒</div>
            <h2 className="text-4xl font-black text-slate-900 tracking-tight">Limite de Consultas Atingido</h2>
            <p className="text-slate-500 text-xl max-w-2xl mx-auto">
              Você utilizou suas 3 consultas gratuitas. Para auditar o imóvel <span className="font-mono font-bold text-slate-900">{carId}</span>:
            </p>

            <div className="max-w-md mx-auto space-y-6 pt-8">
              {showPayment && preferenceId ? (
                <div key="brick-limit-block" className="animate-in fade-in duration-300">
                  <PaymentBrick 
                    preferenceId={preferenceId} 
                    onPaymentSuccess={() => {
                      setPreferenceId(null);
                      setShowPayment(false);
                      performSearch(carId);
                    }} 
                  />
                </div>
              ) : (
                <>
                  <button
                    onClick={handleUnlockReport}
                    className="w-full bg-green-600 hover:bg-green-700 text-white font-black py-6 rounded-2xl shadow-[0_20px_40px_rgba(22,163,74,0.3)] transition-all active:scale-95 text-xl flex items-center justify-center gap-3"
                  >
                    <span>🔓</span> LIBERAR ESTE LAUDO (R$ 150)
                  </button>
                  <div className="relative flex items-center justify-center">
                    <div className="absolute inset-0 flex items-center"><span className="w-full border-t border-slate-200"></span></div>
                    <span className="relative bg-white px-4 text-[10px] font-black uppercase tracking-widest text-slate-400">Ou fale com um especialista</span>
                  </div>
                  <LeadForm carId={carId} />
                </>
              )}
            </div>
          </div>
        )}

        {/* LOADER */}
        {loading && (
          <div className="bg-white p-12 rounded-[2rem] shadow-xl text-center animate-pulse mb-8">
            <p className="text-slate-400 font-black uppercase tracking-widest">Processando Auditoria Geoespacial...</p>
          </div>
        )}

        {/* RESULTADOS */}
        {data && !loading && (
          <section className="grid grid-cols-1 lg:grid-cols-3 gap-8 animate-in fade-in slide-in-from-bottom-10 duration-700">
            <div className="lg:col-span-2 space-y-8">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                
                {/* CARD VEREDITO */}
                <div className={`bg-white p-8 rounded-[2rem] shadow-xl border-t-[12px] ${data.color === 'red' ? 'border-red-500' : data.color === 'orange' ? 'border-orange-500' : 'border-green-500'}`}>
                  <p className="text-slate-400 text-xs font-black uppercase tracking-widest mb-4">Veredito Final</p>
                  <h3 className="text-4xl font-black leading-none text-slate-900">{data.status}</h3>
                  <div className="mt-6 flex items-center gap-2 text-slate-600 font-bold">
                    <span className="bg-slate-100 px-3 py-1 rounded-lg text-sm">{data.city} - {data.uf}</span>
                    <span className="bg-slate-100 px-3 py-1 rounded-lg text-sm">{data.area.toFixed(2)} ha</span>
                  </div>
                </div>

                {/* CARD PASSIVO */}
                <div className="bg-slate-900 p-8 rounded-[2rem] shadow-xl border-t-[12px] border-emerald-400 text-white relative overflow-hidden">
                  <p className="text-emerald-400 text-xs font-black uppercase tracking-widest mb-4">Passivo Financeiro Total</p>
                  {data.isCensored ? (
                    <div className="space-y-2">
                      <h3 className="text-4xl font-black tracking-tighter blur-md select-none">R$ 9.999.999</h3>
                      {/* Substituição da tag <p> pelo <Link> aqui */}
                      <Link href="/caipora/planos" className="block text-center text-[10px] text-emerald-500 font-bold bg-emerald-500/10 hover:bg-emerald-500/20 p-2 rounded border border-emerald-500/20 uppercase tracking-widest transition-colors cursor-pointer">
                        🔒 Contrate o Plano PRO para ver valores
                      </Link>
                    </div>
                  ) : (
                    <>
                      <h3 className="text-4xl font-black tracking-tighter mb-6 break-words leading-tight">{data.liabilityTotal}</h3>
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

              {/* SEÇÃO DE EVIDÊNCIAS E CENSURA */}
              <div className={`bg-white rounded-[2rem] shadow-xl border border-slate-200 overflow-hidden relative ${data.isCensored ? 'min-h-[850px]' : 'min-h-[400px]'}`}>
                <div className="bg-slate-50 border-b p-6 flex justify-between items-center">
                  <h4 className="font-black text-slate-700 text-sm uppercase tracking-widest">Evidências de Auditoria</h4>
                  {!data.isCensored && <span className="text-xs font-mono text-slate-400 bg-white px-4 py-1 rounded-full border border-slate-200">{data.metrics}</span>}
                </div>
                
                <div className={`p-8 space-y-4 ${data.isCensored && !showPayment ? 'blur-md grayscale pointer-events-none select-none' : ''}`}>
                  {showPayment && preferenceId ? (
                    <div key="brick-censored-overlay" className="animate-in zoom-in-95 duration-300">
                      <button 
                        onClick={() => {
                          setShowPayment(false);
                          setPreferenceId(null);
                        }}
                        className="mb-6 text-xs font-bold text-slate-400 hover:text-slate-900 uppercase tracking-widest flex items-center gap-2"
                      >
                        ← Voltar para opções
                      </button>
                      <PaymentBrick 
                        preferenceId={preferenceId} 
                        onPaymentSuccess={() => {
                          setPreferenceId(null);
                          setShowPayment(false);
                          performSearch(carId);
                        }} 
                      />
                    </div>
                  ) : (
                    data.evidenceList.map((text, index) => (
                      <div key={index} className="p-5 rounded-2xl border border-slate-100 bg-slate-50 text-slate-700 font-mono text-lg leading-tight">{text}</div>
                    ))
                  )}
                </div>

                {/* OVERLAY DE CENSURA (PAGAMENTO EM CIMA) */}
                {data.isCensored && !showPayment && (
                  <div className="absolute inset-0 flex flex-col items-center justify-start pt-12 bg-white/40 backdrop-blur-[2px] z-20 p-6">
                    <div className="max-w-md w-full space-y-8">
                      
                      {/* 1. Botão de Pagamento Primeiro */}
                      <div className="space-y-4">
                        <button
                          onClick={handleUnlockReport}
                          className="w-full bg-green-600 hover:bg-green-700 text-white font-black py-6 rounded-2xl shadow-[0_15px_35px_rgba(22,163,74,0.4)] transition-all active:scale-95 flex items-center justify-center gap-3 text-xl"
                        >
                          <span>🔓</span> LIBERAR LAUDO (R$ 150)
                        </button>
                        <p className="text-center text-[10px] text-slate-500 font-bold uppercase tracking-widest">Liberação imediata via Pix ou Cartão</p>
                      </div>

                      {/* 2. Divisor */}
                      <div className="relative flex items-center justify-center">
                        <div className="absolute inset-0 flex items-center"><span className="w-full border-t border-slate-300"></span></div>
                        <span className="relative bg-white/80 px-4 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500">Ou solicite contato comercial</span>
                      </div>

                      {/* 3. Formulário de Lead Embaixo */}
                      <LeadForm carId={carId} />
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* COLUNA DO MAPA */}
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
      <Footer />
    </div>
  );
}
'use client';

import { useState } from 'react';
import Header from '../Header';
import Footer from '../Footer';

export default function PlanosPage() {
  const [loading, setLoading] = useState(false);
  const [email, setEmail] = useState('');

  const handleSubscribe = async () => {
    if (!email || !email.includes('@')) {
      alert("Por favor, insira um e-mail corporativo válido para continuar.");
      return;
    }

    setLoading(true);
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "").replace(/\/$/, "");
      const apiKey = process.env.NEXT_PUBLIC_API_KEY;

      const response = await fetch(`${apiUrl}/payments/create-subscription`, {
        method: "POST",
        headers: { 
          "Content-Type": "application/json", 
          "X-API-Key": apiKey || "" 
        },
        body: JSON.stringify({ email: email })
      });

      const result = await response.json();

      if (response.ok && result.init_point) {
        window.location.href = result.init_point;
      } else {
        const errorMsg = result.detail?.message || result.message || JSON.stringify(result.detail || result);
        if (typeof errorMsg === 'string' && errorMsg.includes("greater than R$ 4000")) {
          alert("⚠️ Limite de transação excedido no Mercado Pago. Entre em contato para faturamento direto.");
        } else {
          alert(`❌ Erro ao gerar pagamento: ${errorMsg}`);
        }
      }
    } catch (error) {
      alert("Falha crítica de conexão. Verifique se a API está online.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex flex-col">
      {/* Header com correção de tipos TS */}
      <Header 
        carId="" 
        setCarId={() => {}} 
        onSearch={() => {}} 
        searchCount={0} 
        isAdmin={false} 
        logout={() => {}} 
      />
      
      <main className="max-w-6xl mx-auto px-6 mt-12 pb-20 relative z-10 flex-grow w-full">
        
        <div className="text-center mb-16 space-y-4">
          <h1 className="text-5xl font-black text-slate-900 tracking-tight uppercase">Escolha seu Plano</h1>
          <p className="text-xl text-slate-500 max-w-2xl mx-auto font-medium">
            Auditoria geoespacial avançada com 24 camadas de dados integradas.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 items-stretch max-w-5xl mx-auto">
          
          {/* CARD FREE - LAYOUT ORIGINAL */}
          <div className="bg-white p-10 rounded-[3rem] shadow-xl border border-slate-200 flex flex-col">
            <h2 className="text-4xl font-black text-slate-900 mb-2 uppercase">Plano Free</h2>
            <p className="text-slate-500 mb-8 text-lg font-medium">Ideal para consultas esporádicas e testes rápidos.</p>
            
            <div className="text-5xl font-black text-slate-900 mb-8 tracking-tighter">R$ 0<span className="text-sm text-slate-400 ml-2 font-normal uppercase tracking-widest">/mês</span></div>
            
            <ul className="space-y-4 mb-8 flex-grow text-slate-600 font-bold">
              <li className="flex items-center gap-3 font-bold text-slate-700">
                  <span className="text-green-500 text-xl">✓</span> Acesso ao Catálogo Sentinela
                </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-xl">✓</span> 3 auditorias detalhadas/mês
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-xl">✓</span> Veredito de Elegibilidade
              </li>
            </ul>

            <button 
              disabled
              className="w-full bg-slate-100 text-slate-400 font-black py-6 rounded-2xl text-xl uppercase tracking-widest cursor-not-allowed"
            >
              Plano Atual
            </button>
          </div>

          {/* CARD PRO - LAYOUT ORIGINAL COM CONTEÚDO NOVO */}
          <div className="bg-slate-900 p-10 rounded-[3rem] shadow-2xl border-4 border-green-500 flex flex-col relative overflow-hidden">
            <div className="absolute top-0 right-0 bg-green-500 text-slate-900 font-black px-8 py-2 rounded-bl-3xl uppercase text-xs tracking-widest">Recomendado</div>
            
            <h2 className="text-4xl font-black text-white mb-2 uppercase">Plano PRO</h2>
            <p className="text-slate-300 mb-8 text-lg font-medium">
              Foco em tradings, compliance e gestores de risco agrícola.
            </p>
            
            <div className="text-5xl font-black text-white tracking-tighter mb-4">R$ 4.000<span className="text-sm text-slate-400 ml-2 font-normal uppercase tracking-widest">/mês</span></div>

            {/* DESTAQUE ART */}
            <div className="bg-green-500/10 border border-green-500/20 p-4 rounded-2xl mb-6">
              <p className="text-green-400 font-black text-xs uppercase tracking-tighter">
                ⭐ BÔNUS: 3 Créditos de Laudo ART/mês inclusos
              </p>
            </div>

            <ul className="space-y-4 mb-8 flex-grow text-slate-300 font-bold">
              <li className="flex items-center gap-3">
                <span className="text-green-400 text-xl">✓</span> 50 Auditorias Automatizadas/mês
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400 text-xl">✓</span> Indicadores EUDR e CMN 5.081
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400 text-xl">✓</span> Mapas GIS e Recortes Forenses
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400 text-xl">✓</span> Mitigação de Adjacência (ANA)
              </li>
              <li className="flex items-center gap-3 text-green-400">
                <span className="text-green-400 text-xl">✓</span> 3 Laudos Periciais assinados (CREA)
              </li>
            </ul>

            <div className="space-y-4 mt-auto">
              <input 
                type="email" 
                placeholder="Seu melhor e-mail corporativo" 
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full px-6 py-4 rounded-2xl bg-slate-800 border border-slate-700 text-white placeholder-slate-400 focus:outline-none focus:border-green-500 focus:ring-1 focus:ring-green-500 transition-all font-bold"
              />
              
              <button 
                onClick={handleSubscribe}
                disabled={loading}
                className="w-full bg-green-500 hover:bg-green-400 text-slate-900 font-black py-6 rounded-2xl transition-all active:scale-95 text-xl uppercase tracking-widest disabled:opacity-70 disabled:cursor-not-allowed flex justify-center items-center gap-2"
              >
                {loading ? (
                  <>
                    <span className="animate-spin h-5 w-5 border-2 border-slate-900 border-t-transparent rounded-full"></span>
                    Gerando Link...
                  </>
                ) : (
                  "Assinar Agora"
                )}
              </button>
            </div>
          </div>

        </div>

        {/* NOTA SOBRE LAUDOS ART ADICIONAIS */}
        <div className="mt-16 text-center space-y-2">
          <p className="text-slate-500 font-bold uppercase text-xs tracking-[0.2em]">Serviço Pericial sob demanda</p>
          <p className="text-slate-400 max-w-xl mx-auto text-sm">
            Laudos periciais avulsos com ART (Resolução 313/86) são entregues em até 7 dias úteis após a solicitação.
          </p>
        </div>
      </main>
      
      <Footer />
    </div>
  );
}
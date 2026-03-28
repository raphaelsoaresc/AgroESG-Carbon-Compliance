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

      console.log("Chamando assinatura para:", `${apiUrl}/payments/create-subscription`);

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
        // Sucesso: Redireciona para o checkout do Mercado Pago
        window.location.href = result.init_point;
      } else {
        console.error("Erro detalhado do servidor:", result);
        
        // Extrai a mensagem de erro de forma segura
        const errorMsg = result.detail?.message || result.message || JSON.stringify(result.detail || result);
        
        // Verifica se é o erro de limite do Mercado Pago
        if (typeof errorMsg === 'string' && errorMsg.includes("greater than R$ 4000")) {
          alert("⚠️ O Mercado Pago bloqueou a transação pois o valor excede o limite atual da conta. Verifique o valor no backend.");
        } else {
          alert(`❌ Erro ao gerar pagamento: ${errorMsg}`);
        }
      }
    } catch (error) {
      console.error("Erro na requisição:", error);
      alert("Falha crítica de conexão. Verifique se a API está online.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900 flex flex-col">
      <Header carId="" setCarId={() => {}} onSearch={() => {}} searchCount={0} />
      
      <main className="max-w-6xl mx-auto px-6 mt-12 pb-20 relative z-10 flex-grow w-full">
        
        <div className="text-center mb-16 space-y-4">
          <h1 className="text-5xl font-black text-slate-900 tracking-tight">Escolha seu Plano</h1>
          <p className="text-xl text-slate-500 max-w-2xl mx-auto">
            Auditoria geoespacial avançada para o agronegócio.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 items-stretch max-w-5xl mx-auto">
          
          {/* CARD FREE */}
          <div className="bg-white p-10 rounded-[3rem] shadow-xl border border-slate-200 flex flex-col">
            <h2 className="text-4xl font-black text-slate-900 mb-2">Plano Free</h2>
            <p className="text-slate-500 mb-8 text-lg">Ideal para consultas esporádicas e testes rápidos.</p>
            
            <div className="text-5xl font-black text-slate-900 mb-8">R$ 0<span className="text-sm text-slate-400 ml-2 font-normal">/mês</span></div>
            
            <ul className="space-y-4 mb-8 flex-grow text-slate-600 font-medium">
              <li className="flex items-center gap-3">
                <span className="text-green-500">✓</span> 3 consultas gratuitas
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500">✓</span> Veredito básico (Elegível/Não Elegível)
              </li>
              <li className="flex items-center gap-3 text-slate-400">
                <span className="text-slate-300">✕</span> Sem indicadores EUDR/CMN 5.081
              </li>
              <li className="flex items-center gap-3 text-slate-400">
                <span className="text-slate-300">✕</span> Sem visualização de mapas e NDVI
              </li>
            </ul>

            <button 
              disabled
              className="w-full bg-slate-100 text-slate-400 font-black py-6 rounded-2xl text-xl uppercase tracking-tight cursor-not-allowed"
            >
              Plano Atual
            </button>
          </div>

          {/* CARD PRO */}
          <div className="bg-slate-900 p-10 rounded-[3rem] shadow-2xl border-4 border-green-500 flex flex-col relative overflow-hidden">
            <div className="absolute top-0 right-0 bg-green-500 text-slate-900 font-black px-8 py-2 rounded-bl-3xl uppercase text-xs tracking-widest">Recomendado</div>
            
            <h2 className="text-4xl font-black text-white mb-2">Plano PRO</h2>
            <p className="text-slate-300 mb-8 text-lg">
              Foco em tradings médias, departamentos de compliance e gestores de risco agrícola.
            </p>
            
            <div className="text-5xl font-black text-white tracking-tighter mb-8">R$ 4.000<span className="text-sm text-slate-400 ml-2 font-normal">/mês</span></div>

            <ul className="space-y-4 mb-8 flex-grow text-slate-300 font-medium">
              <li className="flex items-center gap-3">
                <span className="text-green-400">✓</span> Franquia de 50 consultas/mês
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400">✓</span> Indicadores EUDR e CMN 5.081
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400">✓</span> Visualização de mapas e NDVI
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-400">✓</span> Monitoramento ativo e gestão de risco visual
              </li>
            </ul>

            <div className="space-y-4 mt-auto">
              <input 
                type="email" 
                placeholder="Seu melhor e-mail corporativo" 
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full px-6 py-4 rounded-2xl bg-slate-800 border border-slate-700 text-white placeholder-slate-400 focus:outline-none focus:border-green-500 focus:ring-1 focus:ring-green-500 transition-all"
              />
              
              <button 
                onClick={handleSubscribe}
                disabled={loading}
                className="w-full bg-green-500 hover:bg-green-400 text-slate-900 font-black py-6 rounded-2xl transition-all active:scale-95 text-xl uppercase tracking-tight disabled:opacity-70 disabled:cursor-not-allowed flex justify-center items-center gap-2"
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
      </main>
      
      <Footer />
    </div>
  );
}
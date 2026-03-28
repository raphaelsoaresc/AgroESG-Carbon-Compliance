'use client';
import { useState } from 'react';
import { createClient } from '@supabase/supabase-js';

// Inicializa o cliente do Supabase usando as variáveis que você colocou no .env.local
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
const supabase = createClient(supabaseUrl, supabaseAnonKey);

export default function LeadForm({ carId }: { carId: string }) {
  const [loading, setLoading] = useState(false);
  const [sent, setSent] = useState(false);
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    company: '',
    whatsapp: ''
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    // Insere o lead na tabela que criamos no Supabase
    const { error } = await supabase.from('leads').insert([
      { 
        name: formData.name, 
        email: formData.email, 
        company: formData.company, 
        whatsapp: formData.whatsapp,
        car_id: carId 
      }
    ]);

    if (!error) {
      setSent(true);
    } else {
      console.error("Erro Supabase:", error);
      alert("Erro ao enviar: " + error.message);
    }
    setLoading(false);
  };

  if (sent) {
    return (
      <div className="text-center p-8 bg-white rounded-[2rem] shadow-2xl border border-green-100 animate-in fade-in zoom-in duration-500">
        <div className="w-20 h-20 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-6">
          <span className="text-4xl">✅</span>
        </div>
        <h4 className="text-slate-900 text-xl font-black uppercase tracking-tight">Solicitação Recebida!</h4>
        <p className="text-slate-500 text-sm mt-3 leading-relaxed">
          Nossa equipe técnica foi notificada sobre o interesse no imóvel <br/>
          <strong className="text-slate-900 font-mono text-xs">{carId}</strong>. <br/>
          Entraremos em contato em breve.
        </p>
      </div>
    );
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4 text-left bg-slate-900 p-8 rounded-[2.5rem] border border-white/10 shadow-2xl relative overflow-hidden">
      {/* Detalhe estético de "mira" */}
      <div className="absolute top-0 right-0 p-4 opacity-10 text-4xl">🛰️</div>
      
      <div className="mb-6">
        <h4 className="text-white font-black uppercase tracking-[0.2em] text-sm flex items-center gap-3">
          <span className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></span>
          Desbloquear Auditoria
        </h4>
        <p className="text-slate-400 text-[10px] mt-2 uppercase tracking-widest font-bold">Acesso exclusivo para parceiros Agri-Market</p>
      </div>
      
      <div className="space-y-3">
        <input 
          required
          type="text" 
          placeholder="Nome Completo"
          className="w-full p-4 bg-white/5 border border-white/10 rounded-2xl text-white text-sm outline-none focus:border-green-500 focus:ring-4 focus:ring-green-500/10 transition-all"
          onChange={(e) => setFormData({...formData, name: e.target.value})}
        />
        
        <input 
          required
          type="email" 
          placeholder="E-mail Corporativo"
          className="w-full p-4 bg-white/5 border border-white/10 rounded-2xl text-white text-sm outline-none focus:border-green-500 focus:ring-4 focus:ring-green-500/10 transition-all"
          onChange={(e) => setFormData({...formData, email: e.target.value})}
        />

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <input 
            type="text" 
            placeholder="Empresa"
            className="w-full p-4 bg-white/5 border border-white/10 rounded-2xl text-white text-sm outline-none focus:border-green-500 transition-all"
            onChange={(e) => setFormData({...formData, company: e.target.value})}
          />
          <input 
            required
            type="text" 
            placeholder="WhatsApp"
            className="w-full p-4 bg-white/5 border border-white/10 rounded-2xl text-white text-sm outline-none focus:border-green-500 transition-all"
            onChange={(e) => setFormData({...formData, whatsapp: e.target.value})}
          />
        </div>
      </div>

      <button 
        disabled={loading}
        className="w-full bg-green-500 hover:bg-green-400 text-slate-900 font-black py-5 rounded-2xl text-xs uppercase tracking-[0.2em] transition-all active:scale-95 disabled:opacity-50 shadow-xl shadow-green-500/20"
      >
        {loading ? 'PROCESSANDO...' : 'SOLICITAR ACESSO COMPLETO'}
      </button>
      
      <p className="text-[9px] text-slate-500 text-center mt-4 leading-tight">
        Ao solicitar, você autoriza a Agri-Market Intelligence a entrar em contato para fins comerciais.
      </p>
    </form>
  );
}
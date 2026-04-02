'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { supabase } from '../caipora/lib/supabase';
import { ShieldCheck, Lock, Mail, ArrowRight } from 'lucide-react';

export default function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const router = useRouter();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setErrorMsg(null);

    try {
      const { error } = await supabase.auth.signInWithPassword({
        email,
        password,
      });

      if (error) {
        setErrorMsg(error.message);
        setLoading(false);
      } else {
        // Login com sucesso, redireciona para o dashboard principal
        router.push('/caipora');
      }
    } catch (err) {
      setErrorMsg('Ocorreu um erro inesperado.');
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 flex items-center justify-center p-6 font-sans">
      <div className="w-full max-w-md bg-white rounded-[3rem] shadow-[0_20px_50px_rgba(0,0,0,0.3)] overflow-hidden">
        
        {/* Cabeçalho do Card */}
        <div className="bg-gradient-to-br from-slate-900 via-green-950 to-slate-900 p-10 text-center border-b-4 border-emerald-500">
          <div className="inline-flex p-4 bg-white/10 rounded-3xl mb-4 backdrop-blur-md border border-white/10">
            <ShieldCheck className="w-10 h-10 text-emerald-400" />
          </div>
          <h1 className="text-2xl font-black text-white uppercase tracking-tighter">
            Caipora <span className="text-emerald-500">Sentinela</span>
          </h1>
          <p className="text-emerald-500/60 text-[10px] font-bold uppercase tracking-[0.3em] mt-2">
            Acesso Restrito Master
          </p>
        </div>
        
        {/* Formulário */}
        <form onSubmit={handleLogin} className="p-10 space-y-6">
          {errorMsg && (
            <div className="bg-red-50 border border-red-100 text-red-600 text-xs font-bold p-4 rounded-2xl text-center animate-in fade-in zoom-in duration-300">
              {errorMsg}
            </div>
          )}

          <div className="space-y-2">
            <label className="text-[10px] font-black text-slate-400 ml-2 uppercase tracking-widest">
              E-mail Corporativo
            </label>
            <div className="relative">
              <Mail className="absolute left-4 top-4 w-5 h-5 text-slate-300" />
              <input 
                type="email" 
                required 
                className="w-full p-4 pl-12 bg-slate-50 border-2 border-slate-100 rounded-2xl outline-none focus:border-emerald-500 transition-all font-bold text-slate-700 placeholder:text-slate-300" 
                placeholder="seu@email.com" 
                onChange={e => setEmail(e.target.value)} 
              />
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-[10px] font-black text-slate-400 ml-2 uppercase tracking-widest">
              Senha de Acesso
            </label>
            <div className="relative">
              <Lock className="absolute left-4 top-4 w-5 h-5 text-slate-300" />
              <input 
                type="password" 
                required 
                className="w-full p-4 pl-12 bg-slate-50 border-2 border-slate-100 rounded-2xl outline-none focus:border-emerald-500 transition-all font-bold text-slate-700 placeholder:text-slate-300" 
                placeholder="••••••••" 
                onChange={e => setPassword(e.target.value)} 
              />
            </div>
          </div>

          <button 
            type="submit" 
            disabled={loading} 
            className="w-full bg-slate-900 hover:bg-black text-white p-5 rounded-2xl font-black text-sm uppercase tracking-widest transition-all shadow-xl active:scale-95 disabled:opacity-50 flex items-center justify-center gap-3 group"
          >
            {loading ? (
              <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            ) : (
              <>
                ENTRAR NO PAINEL <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
              </>
            )}
          </button>

          <div className="text-center pt-4">
            <p className="text-[9px] text-slate-400 font-bold uppercase tracking-widest">
              Proteção Geoespacial Ativa &copy; {new Date().getFullYear()}
            </p>
          </div>
        </form>
      </div>
    </div>
  );
}
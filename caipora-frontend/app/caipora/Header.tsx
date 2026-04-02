'use client';
import Image from 'next/image';
import Link from 'next/link';
import { LayoutGrid, Search, CreditCard, LogOut, User } from 'lucide-react';
import { DEMO_IDS } from './types';

interface HeaderProps {
  carId: string;
  setCarId: (id: string) => void;
  onSearch: (id: string) => void;
  searchCount: number;
  isAdmin: boolean; // NOVO
  logout: () => void; // NOVO
}

export default function Header({ carId, setCarId, onSearch, searchCount, isAdmin, logout }: HeaderProps) {
  return (
    <header className="bg-gradient-to-br from-slate-900 via-green-950 to-slate-900 pb-32 pt-16 px-6 text-white shadow-2xl border-b-4 border-green-500">
      <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center gap-12 mb-16">
        {/* Logo com Link para Home */}
        <Link 
          href="/" 
          className="w-40 h-40 md:w-48 md:h-48 relative rounded-[2.5rem] overflow-hidden bg-white shadow-[0_0_60px_rgba(34,197,94,0.4)] border-4 border-white/20 shrink-0 block hover:scale-105 transition-transform cursor-pointer"
        >
          <Image src="/logo-caipora.jpg" alt="Caipora Sentinela" fill className="object-cover" />
        </Link>

        {/* Título e Subtítulo */}
        <div className="flex-1 space-y-4 text-center md:text-left">
          <span className="text-5xl md:text-7xl font-black tracking-tighter block">
            <span className="text-green-400">Caipora</span> Sentinela
          </span>
          <span className="text-slate-400 text-xl font-bold tracking-[0.5em] uppercase">
            Compliance Geoespacial
          </span>
        </div>

        {/* Botões de Navegação Superior */}
        <div className="flex flex-col sm:flex-row gap-3 flex-shrink-0 mt-4 md:mt-0">
          <Link 
            href="/caipora/catalog" 
            className="bg-white/10 hover:bg-white/20 backdrop-blur-md text-white text-xs font-bold px-6 py-4 rounded-2xl transition-all uppercase tracking-widest border border-white/10 shadow-lg flex items-center justify-center gap-2"
          >
            <LayoutGrid className="w-4 h-4 text-green-400" /> Explorar Catálogo
          </Link>
          
          {/* BOTÃO DINÂMICO DE LOGIN / SAIR */}
          {isAdmin ? (
            <button 
              onClick={logout}
              className="bg-red-600/10 hover:bg-red-600/20 text-red-500 text-xs font-bold px-6 py-4 rounded-2xl transition-all uppercase tracking-widest border border-red-500/20 shadow-lg flex items-center justify-center gap-2"
            >
              <LogOut className="w-4 h-4" /> Sair
            </button>
          ) : (
            <Link 
              href="/login" 
              className="bg-white/10 hover:bg-white/20 backdrop-blur-md text-white text-xs font-bold px-6 py-4 rounded-2xl transition-all uppercase tracking-widest border border-white/10 shadow-lg flex items-center justify-center gap-2"
            >
              <User className="w-4 h-4 text-emerald-400" /> Entrar
            </Link>
          )}

          <Link 
            href="/caipora/planos" 
            className="bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-bold px-6 py-4 rounded-2xl transition-all uppercase tracking-widest shadow-lg shadow-emerald-900/20 flex items-center justify-center gap-2"
          >
            <CreditCard className="w-4 h-4" /> Ver Planos
          </Link>
        </div>
      </div>

      {/* Barra de Pesquisa Central */}
      <div className="max-w-4xl mx-auto">
        <div className="relative bg-white p-2 rounded-[2rem] shadow-2xl flex flex-col md:flex-row gap-2">
          <div className="flex-1 flex items-center px-4">
            <Search className="w-6 h-6 text-slate-300 mr-2" />
            <input 
              type="text" 
              value={carId} 
              onChange={(e) => setCarId(e.target.value)}
              placeholder="Digite o código do CAR..."
              className="flex-1 p-4 rounded-2xl text-slate-900 text-xl font-mono outline-none placeholder:text-slate-300"
            />
          </div>
          <button 
            onClick={() => onSearch(carId)} 
            className="bg-slate-900 hover:bg-black text-white px-12 py-6 rounded-2xl font-black text-xl transition-all active:scale-95 flex items-center justify-center gap-3"
          >
            EXECUTAR AUDITORIA
          </button>
          
          {/* Contador de Consultas */}
          <div className="absolute -top-10 right-4 bg-white/10 backdrop-blur-md border border-white/20 px-4 py-1 rounded-full text-[10px] font-bold uppercase tracking-widest">
            Consultas Restantes: <span className={searchCount >= 3 ? "text-red-400" : "text-green-400"}>{3 - searchCount}</span>
          </div>
        </div>
        
        {/* Botões de Demo */}
        <div className="flex justify-center gap-4 mt-8 flex-wrap">
          <button onClick={() => onSearch(DEMO_IDS[0])} className="group flex items-center gap-2 text-xs bg-red-500/10 hover:bg-red-500/20 text-red-200 px-6 py-3 rounded-full border border-red-500/30 transition-all backdrop-blur-sm">
            <span>🔥</span> Demo Risco Crítico
          </button>
          <button onClick={() => onSearch(DEMO_IDS[1])} className="group flex items-center gap-2 text-xs bg-orange-500/10 hover:bg-orange-500/20 text-orange-200 px-6 py-3 rounded-full border border-orange-500/30 transition-all backdrop-blur-sm">
            <span>⚠️</span> Demo Alerta
          </button>
          <button onClick={() => onSearch(DEMO_IDS[2])} className="group flex items-center gap-2 text-xs bg-blue-500/10 hover:bg-blue-500/20 text-blue-200 px-6 py-3 rounded-full border border-blue-500/30 transition-all backdrop-blur-sm">
            <span>🔍</span> Demo Revisão Técnica
          </button>
          <button onClick={() => onSearch(DEMO_IDS[3])} className="group flex items-center gap-2 text-xs bg-green-500/10 hover:bg-green-500/20 text-green-200 px-6 py-3 rounded-full border border-green-500/30 transition-all backdrop-blur-sm">
            <span>✅</span> Demo Conformidade
          </button>
        </div>
      </div>
    </header>
  );
}
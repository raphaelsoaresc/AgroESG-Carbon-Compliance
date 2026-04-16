import Link from 'next/link';
import { ArrowLeft, LayoutGrid } from 'lucide-react';

export function CatalogHeader({ totalCount, startRange, endRange }: any) {
  return (
    <header className="bg-gradient-to-br from-slate-900 via-green-950 to-slate-900 pt-16 pb-32 px-8 text-white border-b-4 border-emerald-500 shadow-2xl">
      <div className="max-w-7xl mx-auto">
        <Link href="/caipora" className="inline-flex items-center gap-2 text-emerald-400 font-bold text-xs mb-8 hover:text-emerald-300 transition-all tracking-widest">
          <ArrowLeft className="w-4 h-4" /> VOLTAR PARA PESQUISA
        </Link>
        <div className="flex flex-col md:flex-row justify-between items-end gap-6">
          <div>
            <h1 className="text-6xl font-black tracking-tighter mb-2 flex items-center gap-4">
              <LayoutGrid className="w-14 h-14 text-emerald-500" /> CATÁLOGO <span className="text-emerald-500">SENTINELA</span>
            </h1>
            <p className="text-slate-400 font-bold uppercase tracking-[0.4em] text-[10px]">Inteligência Territorial e Compliance em Larga Escala</p>
          </div>
          <div className="bg-black/30 backdrop-blur-xl p-6 rounded-[2rem] border border-white/10 flex gap-8">
            <div className="text-center">
              <p className="text-[9px] font-black text-emerald-500 uppercase mb-1">Total na Base</p>
              <p className="text-2xl font-black">{totalCount.toLocaleString('pt-BR')}</p>
            </div>
            <div className="w-px bg-white/10" />
            <div className="text-center">
              <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Visualizando</p>
              <p className="text-2xl font-black text-slate-200">{startRange}-{endRange}</p>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
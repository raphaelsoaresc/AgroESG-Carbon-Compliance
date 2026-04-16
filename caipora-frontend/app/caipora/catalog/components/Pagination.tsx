import { ChevronLeft, ChevronRight } from 'lucide-react';

export function Pagination({ page, totalPages, setPage }: any) {
  return (
    <div className="mt-20 flex flex-col items-center gap-6">
      <div className="flex items-center gap-10">
        <button 
          disabled={page === 1} 
          onClick={() => setPage((p: number) => p - 1)} 
          className="p-5 bg-white border-2 border-slate-100 rounded-3xl shadow-sm hover:border-emerald-500 disabled:opacity-20 transition-all active:scale-90 outline-none"
        >
          <ChevronLeft className="w-8 h-8" />
        </button>
        <div className="flex flex-col items-center">
          <span className="text-[10px] font-black text-slate-400 uppercase tracking-[0.3em] mb-1">Página</span>
          <span className="text-3xl font-black text-slate-900">{page} <span className="text-slate-300">/ {totalPages}</span></span>
        </div>
        <button 
          disabled={page >= totalPages} 
          onClick={() => setPage((p: number) => p + 1)} 
          className="p-5 bg-white border-2 border-slate-100 rounded-3xl shadow-sm hover:border-emerald-500 disabled:opacity-20 transition-all active:scale-90 outline-none"
        >
          <ChevronRight className="w-8 h-8" />
        </button>
      </div>
    </div>
  );
}
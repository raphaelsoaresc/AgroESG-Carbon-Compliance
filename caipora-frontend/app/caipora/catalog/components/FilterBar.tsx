import { Globe, MapPin, Search, Tractor, Fingerprint, Gauge, ShieldCheck, RotateCcw } from 'lucide-react';
import { translateConfidence } from '../../lib/audit-utils';
export function FilterBar({ options, filters, updateFilter, clearFilters }: any) {
  return (
    <div className="bg-white p-8 rounded-[3rem] shadow-2xl border border-slate-100 mb-12 grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-8 gap-4 items-end">
      {/* Bioma */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Globe className="w-3 h-3 text-emerald-500" /> Bioma</label>
        <select value={filters.biome} onChange={(e) => updateFilter('biome', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODOS</option>
          {options.biomes?.map((b: any) => <option key={b} value={b}>{b}</option>)}
        </select>
      </div>

      {/* Estado */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><MapPin className="w-3 h-3 text-emerald-500" /> Estado</label>
        <select value={filters.uf} onChange={(e) => updateFilter('uf', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODOS</option>
          {options.ufs?.map((u: any) => <option key={u} value={u}>{u}</option>)}
        </select>
      </div>

      {/* Cidade */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Search className="w-3 h-3 text-emerald-500" /> Cidade</label>
        <select value={filters.city} onChange={(e) => updateFilter('city', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODAS</option>
          {options.cities?.map((c: any) => <option key={c} value={c}>{c}</option>)}
        </select>
      </div>

      {/* Porte */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Tractor className="w-3 h-3 text-emerald-500" /> Porte</label>
        <select value={filters.producerType} onChange={(e) => updateFilter('producerType', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODOS</option>
          {options.producer_types?.map((t: any) => <option key={t} value={t}>{t}</option>)}
        </select>
      </div>

      {/* Identidade */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Fingerprint className="w-3 h-3 text-emerald-500" /> Identidade</label>
        <select value={filters.identity} onChange={(e) => updateFilter('identity', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODAS</option>
          <option value="Assentamento (Resgate por Localização)">Assentamento (Não declarado)</option>
          <option value="Assentamento (Declarado)">Assentamento (Declarado)</option>
          <option value="Terra Indígena (Resgate por Localização)">Terra Indígena</option>
          <option value="Território Tradicional (Declarado)">Comun. Tradicional</option>
          <option value="Imóvel Rural Privado">Imóvel Privado</option>
        </select>
      </div>

      {/* Precisão */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Gauge className="w-3 h-3 text-emerald-500" /> Precisão</label>
        <select value={filters.confidence} onChange={(e) => updateFilter('confidence', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODAS</option>
          {options.confidences?.map((c: any) => <option key={c} value={c}>{translateConfidence(c)}</option>)}
        </select>
      </div>

      {/* Status CAR */}
      <div className="space-y-2">
        <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><ShieldCheck className="w-3 h-3 text-emerald-500" /> Status CAR</label>
        <select value={filters.carStatus} onChange={(e) => updateFilter('carStatus', e.target.value)} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
          <option value="">TODOS</option>
          {options.car_status?.map((s: any) => <option key={s} value={s}>{s}</option>)}
        </select>
      </div>

      <button onClick={clearFilters} className="p-3 bg-slate-900 text-white rounded-xl font-black text-[9px] tracking-widest hover:bg-black transition-all flex items-center justify-center gap-2 group">
        <RotateCcw className="w-3 h-3 group-hover:rotate-[-90deg] transition-transform" /> LIMPAR
      </button>
    </div>
  );
}
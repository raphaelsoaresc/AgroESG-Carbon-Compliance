'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { Map as MapIcon, ArrowLeft, LayoutGrid, ChevronLeft, ChevronRight, Search, Globe, MapPin, RotateCcw, ShieldCheck, Tractor, Gauge } from 'lucide-react';
import { determineStatusColor, translateConfidence, translateStatus } from '../lib/audit-utils';

export default function CatalogPage() {
  const [properties, setProperties] = useState<any[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  
  // 1. ESTADO E OPÇÕES (Ajustado para car_statuses conforme seu backend)
  const [options, setOptions] = useState({ 
    biomes: [], 
    ufs: [], 
    cities: [], 
    producer_types: [], 
    confidences: [],
    car_statuses: [] // Chave correta do backend
  });

  const [filterStatus, setFilterStatus] = useState('');
  const [filterBiome, setFilterBiome] = useState('');
  const [filterUf, setFilterUf] = useState('');
  const [filterCity, setFilterCity] = useState('');
  const [filterProducerType, setFilterProducerType] = useState('');
  const [filterConfidence, setFilterConfidence] = useState('');
  const [filterCarStatus, setFilterCarStatus] = useState(''); 
  
  const [page, setPage] = useState(1);
  const itemsPerPage = 12;

  const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
  const apiKey = process.env.NEXT_PUBLIC_API_KEY;

  const totalPages = Math.ceil(totalCount / itemsPerPage) || 1;
  const startRange = totalCount === 0 ? 0 : (page - 1) * itemsPerPage + 1;
  const endRange = Math.min(page * itemsPerPage, totalCount);

  // 2. SINCRONIZAÇÃO DE OPÇÕES (useEffect 1)
  useEffect(() => {
    const updateOptions = async () => {
      const params = new URLSearchParams({
        ...(filterBiome && { biome: filterBiome }),
        ...(filterUf && { uf: filterUf }),
        ...(filterCity && { city: filterCity }),
        ...(filterProducerType && { producer_type: filterProducerType }),
        ...(filterConfidence && { confidence: filterConfidence }),
        ...(filterCarStatus && { car_statuses: filterCarStatus }),
      });

      try {
        const res = await fetch(`${apiUrl}/compliance/filter-options?${params}`, {
          headers: { "X-API-Key": apiKey || "" }
        });
        const data = await res.json();
        
        setOptions({
          biomes: data.biomes || [],
          ufs: data.ufs || [],
          cities: data.cities || [],
          producer_types: data.producer_types || [],
          confidences: data.confidences || [],
          car_statuses: data.car_statuses || [] // Mapeamento da chave correta
        });

        // Validação de segurança para resetar filtros se a opção sumir
        if (filterBiome && !data.biomes?.includes(filterBiome)) setFilterBiome('');
        if (filterUf && !data.ufs?.includes(filterUf)) setFilterUf('');
        if (filterCity && !data.cities?.includes(filterCity)) setFilterCity('');
        if (filterProducerType && !data.producer_types?.includes(filterProducerType)) setFilterProducerType('');
        if (filterConfidence && !data.confidences?.includes(filterConfidence)) setFilterConfidence('');
        if (filterCarStatus && !data.car_statuses?.includes(filterCarStatus)) setFilterCarStatus('');
        
      } catch (e) { console.error("Erro na sincronia de filtros", e); }
    };
    updateOptions();
  }, [filterBiome, filterUf, filterCity, filterProducerType, filterConfidence, filterCarStatus]);

  // 3. BUSCA DA LISTA (useEffect 2)
  useEffect(() => {
    const fetchList = async () => {
      setLoading(true);
      const params = new URLSearchParams({
        limit: itemsPerPage.toString(),
        offset: ((page - 1) * itemsPerPage).toString(),
        status: filterStatus,
        biome: filterBiome,
        uf: filterUf,
        city: filterCity,
        producer_type: filterProducerType,
        confidence: filterConfidence,
        car_statuses: filterCarStatus,
      });

      try {
        const response = await fetch(`${apiUrl}/compliance/list?${params}`, {
          headers: { "X-API-Key": apiKey || "", "Content-Type": "application/json" }
        });
        const result = await response.json();
        setProperties(result.items || []);
        setTotalCount(result.total || 0);
      } catch (error) { setProperties([]); }
      finally { setLoading(false); }
    };
    fetchList();
  }, [filterStatus, filterBiome, filterUf, filterCity, filterProducerType, filterConfidence, filterCarStatus, page]);

  return (
    <div className="min-h-screen bg-[#F8FAFC] font-sans text-slate-900">
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

      <main className="max-w-7xl mx-auto px-8 -mt-20 pb-20">
        {/* BARRA DE FILTROS (7 COLUNAS) */}
        <div className="bg-white p-8 rounded-[3rem] shadow-2xl border border-slate-100 mb-12 grid grid-cols-1 md:grid-cols-3 lg:grid-cols-7 gap-4 items-end">
          
          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Globe className="w-3 h-3 text-emerald-500" /> Bioma</label>
            <select value={filterBiome} onChange={(e) => { setFilterBiome(e.target.value); setPage(1); }} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
              <option value="">TODOS</option>
              {(options.biomes || []).map(b => <option key={b} value={b}>{b}</option>)}
            </select>
          </div>

          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><MapPin className="w-3 h-3 text-emerald-500" /> Estado</label>
            <select value={filterUf} onChange={(e) => { setFilterUf(e.target.value); setPage(1); }} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
              <option value="">TODOS</option>
              {(options.ufs || []).map(u => <option key={u} value={u}>{u}</option>)}
            </select>
          </div>

          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Search className="w-3 h-3 text-emerald-500" /> Cidade</label>
            <select value={filterCity} onChange={(e) => { setFilterCity(e.target.value); setPage(1); }} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
              <option value="">TODAS</option>
              {(options.cities || []).map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>

          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Tractor className="w-3 h-3 text-emerald-500" /> Produtor</label>
            <select value={filterProducerType} onChange={(e) => { setFilterProducerType(e.target.value); setPage(1); }} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
              <option value="">TODOS</option>
              {(options.producer_types || []).map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>

          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2"><Gauge className="w-3 h-3 text-emerald-500" /> Precisão</label>
            <select value={filterConfidence} onChange={(e) => { setFilterConfidence(e.target.value); setPage(1); }} className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all">
              <option value="">TODAS</option>
              {(options.confidences || []).map(c => <option key={c} value={c}>{translateConfidence(c)}</option>)}
            </select>
          </div>

          {/* FILTRO STATUS CAR (Ajustado para car_statuses) */}
          <div className="space-y-2">
            <label className="text-[9px] font-black text-slate-400 ml-2 uppercase tracking-widest flex items-center gap-2">
              <ShieldCheck className="w-3 h-3 text-emerald-500" /> Status CAR
            </label>
            <select 
              value={filterCarStatus} 
              onChange={(e) => { setFilterCarStatus(e.target.value); setPage(1); }} 
              className="w-full p-3 bg-slate-50 border-2 border-slate-100 rounded-xl text-[11px] font-black text-slate-700 outline-none focus:border-emerald-500 transition-all"
            >
              <option value="">TODOS</option>
              {(options.car_statuses || []).map(s => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          <button onClick={() => { setFilterBiome(''); setFilterUf(''); setFilterCity(''); setFilterStatus(''); setFilterProducerType(''); setFilterConfidence(''); setFilterCarStatus(''); setPage(1); }} className="p-3 bg-slate-900 text-white rounded-xl font-black text-[9px] tracking-widest hover:bg-black transition-all flex items-center justify-center gap-2 group">
            <RotateCcw className="w-3 h-3 group-hover:rotate-[-90deg] transition-transform" /> LIMPAR
          </button>
        </div>

        {/* BOTÕES DE STATUS */}
        <div className="flex flex-wrap gap-3 mb-12">
          <button onClick={() => {setFilterStatus(''); setPage(1);}} className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filterStatus === '' ? 'bg-slate-900 text-white border-black' : 'bg-slate-100 text-slate-500 border-transparent'}`}>TODOS</button>
          <button onClick={() => {setFilterStatus('CONFORME_VERDE'); setPage(1);}} className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filterStatus === 'CONFORME_VERDE' ? 'bg-emerald-600 text-white border-emerald-800' : 'bg-emerald-50 text-emerald-600 border-transparent'}`}>CONFORMES</button>
          <button onClick={() => {setFilterStatus('BLOQUEADO_VERMELHO'); setPage(1);}} className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filterStatus === 'BLOQUEADO_VERMELHO' ? 'bg-red-600 text-white border-red-800' : 'bg-red-50 text-red-600 border-transparent'}`}>BLOQUEADOS</button>
          <button onClick={() => {setFilterStatus('ALERTA_LARANJA'); setPage(1);}} className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filterStatus === 'ALERTA_LARANJA' ? 'bg-orange-500 text-white border-orange-700' : 'bg-orange-50 text-orange-600 border-transparent'}`}>ALERTAS</button>
          <button onClick={() => {setFilterStatus('REVISAO_AZUL'); setPage(1);}} className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filterStatus === 'REVISAO_AZUL' ? 'bg-blue-600 text-white border-blue-800' : 'bg-blue-50 text-blue-600 border-transparent'}`}>REVISÃO</button>
        </div>

        {/* GRID DE CARDS */}
        {loading ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 animate-pulse">
            {[1, 2, 3].map(i => <div key={i} className="h-72 bg-slate-200 rounded-[3rem]" />)}
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
              {properties.map((prop) => {
                const color = determineStatusColor(prop.final_eligibility_status, prop.geospatial_confidence_level);
                const confidenceLabel = translateConfidence(prop.geospatial_confidence_level).split(':')[0];
                const translatedStatus = translateStatus(prop.final_eligibility_status);
                
                return (
                  <div key={prop.property_id} onClick={() => window.location.href = `/caipora?car=${prop.property_id}`} className="group cursor-pointer bg-white p-8 rounded-[3rem] border-2 border-slate-100 hover:border-emerald-500 hover:shadow-2xl transition-all duration-500 flex flex-col justify-between">
                    <div>
                      <div className="flex justify-between items-center mb-8">
                        <div className="flex gap-2">
                          <div className={`px-3 py-1.5 rounded-xl text-[9px] font-black uppercase tracking-tighter border-2 ${
                            color === 'red' ? 'bg-red-500 text-white border-red-400' : 
                            color === 'orange' ? 'bg-orange-500 text-white border-orange-400' :
                            color === 'blue' ? 'bg-blue-500 text-white border-blue-400' : 'bg-emerald-600 text-white border-emerald-500'
                          }`}>
                            {translatedStatus}
                          </div>
                          
                          <div className="px-3 py-1.5 rounded-xl text-[9px] font-black uppercase tracking-tighter border-2 bg-slate-100 text-slate-500 border-slate-200 flex items-center gap-1">
                            <ShieldCheck className={`w-3 h-3 ${prop.geospatial_confidence_level.includes('HIGH') ? 'text-emerald-500' : 'text-amber-500'}`} />
                            {confidenceLabel}
                          </div>
                        </div>
                      </div>
                      
                      <h3 className="text-2xl font-mono font-black text-slate-900 break-all leading-none mb-2 group-hover:text-emerald-600 transition-colors">{prop.property_id}</h3>
                      <div className="flex items-center gap-2">
                        <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{prop.biome_name}</p>
                        <span className="text-slate-200">•</span>
                        <p className="text-[10px] font-black text-emerald-600 uppercase tracking-widest">{prop.producer_size_category}</p>
                      </div>
                    </div>
                    
                    <div className="flex items-center justify-between pt-6 mt-8 border-t border-slate-50">
                      <div className="text-[11px] font-black text-slate-500 uppercase tracking-tighter">{prop.city} / {prop.uf_origem}</div>
                      <div className="text-slate-900 font-black text-sm">{prop.area_ha.toLocaleString('pt-BR')} <span className="text-[9px] text-slate-400">HA</span></div>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* PAGINAÇÃO */}
            <div className="mt-20 flex flex-col items-center gap-6">
              <div className="flex items-center gap-10">
                <button disabled={page === 1} onClick={() => setPage(p => p - 1)} className="p-5 bg-white border-2 border-slate-100 rounded-3xl shadow-sm hover:border-emerald-500 disabled:opacity-20 transition-all active:scale-90 outline-none">
                  <ChevronLeft className="w-8 h-8" />
                </button>
                <div className="flex flex-col items-center">
                  <span className="text-[10px] font-black text-slate-400 uppercase tracking-[0.3em] mb-1">Página</span>
                  <span className="text-3xl font-black text-slate-900">{page} <span className="text-slate-300">/ {totalPages}</span></span>
                </div>
                <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)} className="p-5 bg-white border-2 border-slate-100 rounded-3xl shadow-sm hover:border-emerald-500 disabled:opacity-20 transition-all active:scale-90 outline-none">
                  <ChevronRight className="w-8 h-8" />
                </button>
              </div>
            </div>
          </>
        )}
      </main>
    </div>
  );
}
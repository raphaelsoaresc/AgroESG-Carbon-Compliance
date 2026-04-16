'use client';

import { useCatalogData } from './hooks/useCatalogData';
import { CatalogHeader } from './components/CatalogHeader';
import { FilterBar } from './components/FilterBar';
import { PropertyCard } from './components/PropertyCard';
import { Pagination } from './components/Pagination';

export default function CatalogPage() {
  const {
    properties, totalCount, loading, page, setPage,
    options, filters, updateFilter, clearFilters,
    itemsPerPage, totalPages
  } = useCatalogData();

  const startRange = totalCount === 0 ? 0 : (page - 1) * itemsPerPage + 1;
  const endRange = Math.min(page * itemsPerPage, totalCount);

  return (
    <div className="min-h-screen bg-[#F8FAFC] font-sans text-slate-900">
      <CatalogHeader totalCount={totalCount} startRange={startRange} endRange={endRange} />

      <main className="max-w-7xl mx-auto px-8 -mt-20 pb-20">
        <FilterBar 
          options={options} 
          filters={filters} 
          updateFilter={updateFilter} 
          clearFilters={clearFilters} 
        />

        {/* BOTÕES DE STATUS (CUMULATIVOS) */}
        <div className="flex flex-wrap gap-3 mb-12">
          {[
            { id: '', label: 'TODOS', color: 'bg-slate-900', active: 'bg-slate-900 text-white border-black', inactive: 'bg-slate-100 text-slate-500' },
            { id: 'CONFORME_VERDE', label: 'CONFORMES', color: 'bg-emerald-600', active: 'bg-emerald-600 text-white border-emerald-800', inactive: 'bg-emerald-50 text-emerald-600' },
            { id: 'BLOQUEADO_VERMELHO', label: 'BLOQUEADOS', color: 'bg-red-600', active: 'bg-red-600 text-white border-red-800', inactive: 'bg-red-50 text-red-600' },
            { id: 'ALERTA_LARANJA', label: 'ALERTAS', color: 'bg-orange-500', active: 'bg-orange-500 text-white border-orange-700', inactive: 'bg-orange-50 text-orange-600' },
            { id: 'REVISAO_AZUL', label: 'REVISÃO', color: 'bg-blue-600', active: 'bg-blue-600 text-white border-blue-800', inactive: 'bg-blue-50 text-blue-600' },
          ].map((btn) => (
            <button 
              key={btn.id}
              onClick={() => updateFilter('status', btn.id)} 
              className={`px-8 py-4 rounded-2xl text-[10px] font-black tracking-widest border-2 transition-all ${filters.status === btn.id ? btn.active : btn.inactive + ' border-transparent'}`}
            >
              {btn.label}
            </button>
          ))}
        </div>

        {loading ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 animate-pulse">
            {[1, 2, 3].map(i => <div key={i} className="h-72 bg-slate-200 rounded-[3rem]" />)}
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
              {properties.map((prop) => (
                <PropertyCard key={prop.property_id} prop={prop} />
              ))}
            </div>

            <Pagination page={page} totalPages={totalPages} setPage={setPage} />
          </>
        )}
      </main>
    </div>
  );
}
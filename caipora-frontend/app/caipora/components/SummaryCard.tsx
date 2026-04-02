import { Map as MapIcon, ShieldCheck, ShieldAlert } from 'lucide-react';
import { AuditData } from '../types';
import { getSizeBadgeStyle } from '../lib/audit-utils';

export const SummaryCard = ({ data }: { data: AuditData }) => {
  const isBlocked = data.isTechnicallyBlocked;

  return (
    <div className="bg-white rounded-3xl shadow-sm border border-slate-200 p-8 flex flex-col justify-between relative overflow-hidden">
      <div className={`absolute top-0 left-0 w-1.5 h-full ${
        data.color === 'red' ? 'bg-red-500' : data.color === 'orange' ? 'bg-orange-500' : data.color === 'blue' ? 'bg-blue-500' : 'bg-emerald-500'
      }`} />
      
      {/* Selo de Compliance e Tamanho no topo */}
      <div className="flex justify-between items-start mb-6">
        <div className={`px-3 py-1 rounded-full text-[10px] font-bold tracking-widest uppercase flex items-center gap-2 ${
          !isBlocked ? 'bg-emerald-50 text-emerald-600 border border-emerald-100' : 'bg-red-50 text-red-600 border border-red-100'
        }`}>
          {!isBlocked ? <ShieldCheck className="w-3 h-3" /> : <ShieldAlert className="w-3 h-3" />}
          {!isBlocked ? 'Compliance OK' : 'Não Compliance'}
        </div>

        {/* Badge de Tamanho Fundiário */}
        <div className={`px-3 py-1 rounded-full text-[10px] font-bold tracking-widest uppercase border ${getSizeBadgeStyle(data.producerSizeCategory || '')}`}>
          {data.producerSizeCategory || 'NÃO CLASSIFICADO'}
        </div>
      </div>

      <div>
        <span className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-4 block">
          Relatório de Auditoria: {data.carNumber}
        </span>
        <h2 className="text-xl font-semibold text-slate-800 leading-snug mb-6 italic">"{data.analysisReason}"</h2>
      </div>

      <div className="flex flex-wrap gap-2">
        <span className={`inline-flex items-center px-3 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider ${
          data.color === 'blue' ? 'bg-blue-50 text-blue-600 border border-blue-100' : data.color === 'orange' ? 'bg-orange-50 text-orange-600 border border-orange-100' : 'bg-slate-100 text-slate-600'
        }`}>Confiança: {data.confidenceLevel || 'N/A'}</span>
        <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-slate-100 text-slate-600">
          <MapIcon className="w-3 h-3 mr-1.5" /> {data.city} - {data.uf}
        </span>
        <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-slate-100 text-slate-600">{data.area.toFixed(2)} ha</span>
      </div>
    </div>
  );
};
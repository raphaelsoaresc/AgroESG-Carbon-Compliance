import React from 'react';
import { MapPin, ShieldCheck, ShieldAlert, Info, Fingerprint } from 'lucide-react';
import { AuditData } from '../types';
import { 
  getSizeBadgeStyle, 
  getStatusBadge, 
  getAnalysisReason, 
  translateConfidence 
} from '../lib/audit-utils';

export const SummaryCard = ({ data }: { data: AuditData }) => {
  const statusBadge = getStatusBadge(data.is_technically_blocked, data.final_eligibility_status || '');
  const analysisReason = getAnalysisReason(data.final_eligibility_status || '', data.geospatial_confidence_level || '');
  const confidenceLabel = translateConfidence(data.geospatial_confidence_level || '');

  return (
    <div className="bg-white rounded-[2rem] shadow-[0_8px_30px_rgb(0,0,0,0.04)] border border-slate-100 p-8 flex flex-col justify-between relative overflow-hidden min-h-[420px]">
      
      {/* Barra Lateral de Status */}
      <div className={`absolute top-0 left-0 w-2 h-full ${
        data.color === 'red' ? 'bg-red-500' : 
        data.color === 'orange' ? 'bg-orange-500' : 
        data.color === 'blue' ? 'bg-blue-500' : 'bg-emerald-500'
      }`} />
      
      {/* Header: Status vs Tamanho */}
      <div className="flex justify-between items-start mb-8 gap-4">
        {/* STATUS: Corrigido para aceitar Orange/Laranja */}
        <div className={`flex items-center gap-2 px-4 py-2 rounded-full border text-[11px] font-bold tracking-wider uppercase ${
          statusBadge.color === 'red' ? 'bg-red-50 text-red-600 border-red-100' :
          statusBadge.color === 'orange' ? 'bg-orange-50 text-orange-600 border-orange-100' : // ADICIONADO
          statusBadge.color === 'green' ? 'bg-emerald-50 text-emerald-600 border-emerald-100' :
          'bg-blue-50 text-blue-600 border-blue-100'
        }`}>
          {/* Ícone: ShieldAlert para Vermelho e Laranja */}
          {(statusBadge.color === 'red' || statusBadge.color === 'orange') ? <ShieldAlert className="w-4 h-4" /> : <ShieldCheck className="w-4 h-4" />}
          {statusBadge.label}
        </div>

        {/* TAMANHO */}
        <div className={`px-4 py-2 rounded-full border text-[10px] font-black tracking-widest uppercase shadow-sm ${getSizeBadgeStyle(data.producer_size_category || '')}`}>
          {data.producer_size_category || 'N/A'}
        </div>
      </div>

      {/* Conteúdo Principal */}
      <div className="flex-grow">
        <div className="bg-slate-50 rounded-2xl p-4 mb-6 border border-slate-100">
          <div className="flex items-center gap-2 text-slate-400 mb-1">
            <Fingerprint className="w-3.5 h-3.5" />
            <span className="text-[10px] font-bold uppercase tracking-[0.2em]">Código do CAR</span>
          </div>
          <p className="text-[13px] font-mono font-bold text-slate-700 break-all leading-tight">
            {data.propertyId}
          </p>
        </div>
        
        <div className="relative">
          <span className="text-slate-200 text-5xl font-serif absolute -top-6 -left-2 select-none">“</span>
          <h2 className="text-xl font-semibold text-slate-900 leading-snug tracking-tight relative z-10 pl-4 italic">
            {analysisReason}
          </h2>
        </div>
      </div>

      {/* Footer */}
      <div className="grid grid-cols-1 gap-3 mt-8">
        <div className="flex items-center gap-3 bg-slate-50 border border-slate-100 rounded-2xl p-3">
          <div className="bg-white p-2 rounded-xl shadow-sm">
            <Info className="w-4 h-4 text-slate-400" />
          </div>
          <div>
            <p className="text-[9px] font-bold text-slate-400 uppercase tracking-wider">Nível de Confiança</p>
            <p className="text-xs font-bold text-slate-700 uppercase">{confidenceLabel}</p>
          </div>
        </div>

        <div className="flex gap-3">
          <div className="flex-[1.5] flex items-center gap-3 bg-slate-50 border border-slate-100 rounded-2xl p-3">
            <MapPin className="w-4 h-4 text-slate-400" />
            <span className="text-xs font-bold text-slate-600 truncate">
              {data.city || 'N/A'} - {data.uf_origem || 'N/A'}
            </span>
          </div>
          <div className="flex-1 flex items-center gap-3 bg-slate-50 border border-slate-100 rounded-2xl p-3">
            <div className="w-1.5 h-1.5 rounded-full bg-slate-400" />
            <span className="text-xs font-bold text-slate-600">
              {(data.area_ha || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2 })} ha
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};
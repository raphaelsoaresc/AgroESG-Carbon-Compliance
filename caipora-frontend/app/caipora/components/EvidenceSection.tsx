import React from 'react';
import { 
  AlertTriangle, ExternalLink, ArrowRight, 
  Landmark, Users, Leaf, HardHat, Info,
  ShieldCheck, Lock, Search, MapPin, Scale
} from 'lucide-react';
import { AuditData } from '../types';
import { 
  formattedDate, 
  getSectionStatusStyle 
} from '../lib/audit-utils';

export const EvidenceSection = ({ data, onUnlock, carId }: { data: AuditData, onUnlock: () => void, carId: string }) => {
  
  // Agora usamos os ARRAYS nativos enviados pelo backend para maior precisão
  const adminEvidences = [
    ...(data.risk_analysis?.evidence_admin_array || []).filter(item => !item.toLowerCase().includes('original') && !item.toLowerCase().includes('processada')),
    ...(data.environmental_score?.is_liability_uncertain ? [`[FRAUDE] DISCREPÂNCIA CRÍTICA: Área cartográfica muito superior à declarada.`] : []),
    ...(data.car_status !== data.car_status_spatial && data.car_status_spatial ? [`[DIVERGÊNCIA] Status na base geométrica difere do alfanumérico: ${data.car_status_spatial}.`] : []),
  ];

  const fh = (val: number | undefined) => (val || 0).toLocaleString('pt-BR', { minimumFractionDigits: 3 }) + ' ha';

  const environmentalEvidences = [
    ...(data.risk_analysis?.evidence_environmental_array || []),
    ...((data.deforestation_metrics?.mapbiomas_deforested_ha ?? 0) > 0.1 ? [
      `[ALERTA] Desmatamento detectado: ${fh(data.deforestation_metrics?.mapbiomas_deforested_ha)}. 
       (Área bruta original: ${fh(data.deforestation_metrics?.mapbiomas_deforested_ha_raw)})`
    ] : []),
    ...((data.environmental_score?.rl_deficit_ha ?? 0) > 0.01 ? [`[PASSIVO] Déficit de Reserva Legal: ${fh(data.environmental_score?.rl_deficit_ha)}.`] : []),
  ];

  const socialEvidences = [
    ...(data.risk_analysis?.evidence_social_array || []),
    ...((data.social_score?.forensic_ti_ha ?? 0) > 0.01 ? [`[FUNAI] Sobreposição em Terra Indígena${data.social_score?.ti_name ? `: ${data.social_score.ti_name}` : ''}.`] : []),
    ...((data.social_score?.forensic_uc_ha ?? 0) > 0.01 ? [`[ICMBIO] Sobreposição em Unidade de Conservação${data.social_score?.uc_name ? `: ${data.social_score.uc_name}` : ''}.`] : []),
    ...((data.social_score?.forensic_settlement_ha ?? 0) > 0.01 ? [`[INCRA] Sobreposição em Assentamento${data.social_score?.settlement_name ? `: ${data.social_score.settlement_name}` : ''}.`] : []),
    ...((data.social_score?.slave_labor_overlap_ha ?? 0) > 0 ? [`[MTE] Vínculo com Trabalho Escravo (Lista Suja).`] : []),
  ];

  const infraEvidences = [
    ...(data.risk_analysis?.evidence_infrastructure_array || []),
    ...(data.max_slope_degrees > 45 ? [`[ALERTA] Declividade crítica (${data.max_slope_degrees.toFixed(2)}°).`] : []),
    ...(data.risk_analysis?.is_cmn_5081_sensitive ? [`[RESTRIÇÃO CRÉDITO] Imóvel enquadrado na Resolução CMN 5.081 (Embargo em Bioma Amazônia).`] : []),
  ];

  const technicalLogs = (data.risk_analysis?.evidence_admin_array || []).filter(item => 
    item.toLowerCase().includes('original') || item.toLowerCase().includes('processada')
  );

  return (
    <div className="bg-white rounded-[2.5rem] shadow-sm border border-slate-200 overflow-hidden relative">
      
      {/* HEADER: FOCO EM CONFIABILIDADE */}
      <div className="px-10 py-8 border-b border-slate-100 bg-slate-900">
        <div className="flex flex-col md:flex-row justify-between items-center gap-6">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-white/5 rounded-2xl">
              <Search className="w-6 h-6 text-slate-400" />
            </div>
            <div>
              <h3 className="text-xs font-black text-slate-500 uppercase tracking-[0.2em]">Laudo Técnico de Auditoria</h3>
              <p className="text-white font-mono text-sm opacity-60">{carId}</p>
            </div>
          </div>
          
          <div className="flex items-center gap-12">
            <div className="text-right">
              <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1">Acurácia dos Dados Públicos</p>
              <p className="text-2xl font-black text-emerald-400">{data.data_reliability_index}%</p>
            </div>
            <div className="text-right border-l border-white/10 pl-12">
              <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1">Processamento</p>
              <p className="text-sm font-bold text-white">{formattedDate(data.processed_at)}</p>
            </div>
          </div>
        </div>
      </div>

      <div className="p-10 space-y-12 relative">
        
        {/* 01. AUDITORIA CARTOGRÁFICA */}
        <section>
          <div className="flex items-center gap-2 mb-6 text-slate-900">
            <Landmark className="w-4 h-4" />
            <h4 className="text-xs font-black uppercase tracking-widest">01. Identificação e Auditoria Cartográfica</h4>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className={`p-4 rounded-xl border transition-colors ${getSectionStatusStyle(data.car_status)}`}>
              <p className="text-[9px] font-black uppercase opacity-70 mb-1">Status (Sicar)</p>
              <p className="text-sm font-black">{data.car_status || 'N/A'}</p>
              {data.car_status_spatial && data.car_status_spatial !== data.car_status && (
                <p className="text-[8px] mt-1 font-bold text-red-600 uppercase">Geometria: {data.car_status_spatial}</p>
              )}
            </div>
            <DataBox label="Tipo de Imóvel" value={data.property_identity_type} />
            <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
              <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Localização Central</p>
              <div className="flex items-center gap-1 text-slate-700">
                <MapPin className="w-3 h-3 text-slate-400" />
                <p className="text-[10px] font-mono font-bold">{data.latitude?.toFixed(4)}, {data.longitude?.toFixed(4)}</p>
              </div>
            </div>
          </div>

          <div className="bg-slate-50 border border-slate-200 rounded-3xl p-6 mb-6 flex flex-col md:flex-row items-center justify-around gap-6">
            <div className="text-center">
              <p className="text-[10px] font-bold text-slate-400 uppercase mb-1">Área Declarada</p>
              <p className="text-lg font-mono text-slate-400 line-through">{fh(data.area_ha)}</p>
            </div>
            <ArrowRight className="w-5 h-5 text-slate-300 hidden md:block" />
            <div className="text-center px-8 py-4 bg-white rounded-2xl border border-slate-200 shadow-sm">
              <p className="text-[10px] font-black text-emerald-600 uppercase mb-1">Área Real Processada</p>
              <p className="text-2xl font-mono font-black text-slate-900">{fh(data.area_geometria_ha)}</p>
            </div>
            <div className="text-center">
              <p className="text-[10px] font-bold text-slate-400 uppercase mb-1">Módulos Fiscais</p>
              <p className="text-lg font-mono text-slate-600 font-bold">{data.fiscal_modules?.toFixed(2)} MF</p>
            </div>
          </div>
          <EvidenceList items={adminEvidences} />
        </section>

        {/* BLOCO CENSURADO */}
        <div className="relative">
          {data.isCensored && (
            <div className="absolute inset-0 z-20 flex flex-col items-center justify-center bg-white/40 backdrop-blur-md rounded-3xl border border-slate-200 p-12 text-center">
              <div className="bg-slate-900 w-16 h-16 rounded-2xl flex items-center justify-center mb-6 shadow-lg">
                <Lock className="text-emerald-400 w-8 h-8" />
              </div>
              <h3 className="text-2xl font-black text-slate-900 mb-2 uppercase tracking-tighter">Laudo Forense Bloqueado</h3>
              <button 
                onClick={onUnlock}
                className="px-8 py-4 bg-slate-900 text-white rounded-xl font-black uppercase tracking-widest text-[10px] hover:bg-slate-800 transition-all shadow-xl"
              >
                Liberar Acesso Completo
              </button>
            </div>
          )}

          <div className={`space-y-12 ${data.isCensored ? 'blur-md pointer-events-none select-none' : ''}`}>
            {/* 02. AMBIENTAL */}
            <section>
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-2 text-slate-900">
                  <Leaf className="w-4 h-4" />
                  <h4 className="text-xs font-black uppercase tracking-widest">02. Diagnóstico Ambiental Detalhado</h4>
                </div>
                {data.environmental_score?.reference_forest_code_date && (
                  <div className="flex items-center gap-1.5 px-3 py-1 bg-emerald-50 border border-emerald-100 rounded-full">
                    <Scale className="w-3 h-3 text-emerald-600" />
                    <span className="text-[9px] font-black text-emerald-700 uppercase">Marco Legal: 22/07/2008</span>
                  </div>
                )}
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <DataBox label="Bioma" value={data.environmental_score?.biome_name} />
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Desmatamento</p>
                  <p className="text-sm font-black text-slate-700">{fh(data.deforestation_metrics?.mapbiomas_deforested_ha)}</p>
                  <p className="text-[8px] text-slate-400 font-bold">BRUTO: {fh(data.deforestation_metrics?.mapbiomas_deforested_ha_raw)}</p>
                </div>
                <DataBox label="Déficit de RL" value={fh(data.environmental_score?.rl_deficit_ha)} />
                <DataBox label="Restrição EUDR" value={data.environmental_score?.is_eudr_restricted ? 'SIM' : 'NÃO'} />
              </div>
              <EvidenceList items={environmentalEvidences} url={data.deforestation_metrics?.official_reports_urls} />
            </section>

            {/* 03. SOCIAL */}
            <section>
              <div className="flex items-center gap-2 mb-6 text-slate-900">
                <Users className="w-4 h-4" />
                <h4 className="text-xs font-black uppercase tracking-widest">03. Sobreposições e Conflitos</h4>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Terra Indígena</p>
                  <p className="text-sm font-black text-slate-700">{fh(data.social_score?.forensic_ti_ha)}</p>
                  {data.social_score?.ti_name && <p className="text-[8px] text-emerald-600 font-black uppercase truncate">{data.social_score.ti_name}</p>}
                </div>
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Unid. Conservação</p>
                  <p className="text-sm font-black text-slate-700">{fh(data.social_score?.forensic_uc_ha)}</p>
                  {data.social_score?.uc_name && <p className="text-[8px] text-emerald-600 font-black uppercase truncate">{data.social_score.uc_name}</p>}
                </div>
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Assentamentos</p>
                  <p className="text-sm font-black text-slate-700">{fh(data.social_score?.forensic_settlement_ha)}</p>
                  {data.social_score?.settlement_name && <p className="text-[8px] text-emerald-600 font-black uppercase truncate">{data.social_score.settlement_name}</p>}
                </div>
                <DataBox label="Trabalho Escravo" value={(data.social_score?.slave_labor_overlap_ha ?? 0) > 0 ? 'DETECTADO' : 'NADA CONSTA'} />
              </div>
              <EvidenceList items={socialEvidences} />
            </section>

            {/* 04. INFRAESTRUTURA */}
            <section>
              <div className="flex items-center gap-2 mb-6 text-slate-900">
                <HardHat className="w-4 h-4" />
                <h4 className="text-xs font-black uppercase tracking-widest">04. Logística e Vizinhança</h4>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <DataBox label="Declividade Máxima" value={`${data.max_slope_degrees.toFixed(2)}°`} />
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Risco Estruturado</p>
                  <p className="text-sm font-black text-slate-700">{data.risk_analysis?.is_structured_environmental_risk ? 'ALTO' : 'BAIXO'}</p>
                </div>
                <DataBox label="Corpos d'água artificiais" value={data.risk_analysis?.count_artificial_water_bodies} />
                <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
                  <p className="text-[9px] font-black text-slate-400 uppercase mb-1">Score Adjacência</p>
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-black text-slate-700">{data.risk_analysis?.max_adjacency_score}</span>
                    {data.risk_analysis?.has_physical_barrier && (
                      <span title="Risco Mitigado por Barreira Física">
                        <ShieldCheck className="w-3 h-3 text-emerald-500" />
                      </span>
                    )}
                  </div>
                </div>
              </div>
              <EvidenceList items={infraEvidences} />
            </section>
          </div>
        </div>
      </div>

      {/* RODAPÉ TÉCNICO: LOGS */}
      {!data.isCensored && technicalLogs.length > 0 && (
        <div className="px-10 py-8 border-t border-slate-100 bg-slate-50/50">
          <div className="flex items-center gap-2 mb-4 text-slate-400">
            <Info className="w-4 h-4" />
            <span className="text-[10px] font-black uppercase tracking-widest">Logs de Auditoria Cartográfica</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {technicalLogs.map((log, i) => (
              <div key={i} className="text-[11px] font-mono text-slate-500 bg-white border border-slate-100 px-4 py-2 rounded-lg shadow-sm">{log}</div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

const DataBox = ({ label, value }: { label: string, value: string | number | undefined }) => (
  <div className="p-4 rounded-xl border border-slate-100 bg-slate-50">
    <p className="text-[9px] font-black text-slate-400 uppercase tracking-tighter mb-1">{label}</p>
    <p className="text-sm font-black text-slate-700 break-words">{value || 'N/A'}</p>
  </div>
);

const EvidenceList = ({ items, url }: { items: string[], url?: string | null }) => {
  if (items.length === 0) return null; 

  const urls = url ? url.split('|').filter(u => u.trim() !== "") : [];

  return (
    <div className="space-y-2 mt-4">
      {items.map((text, i) => (
        <div key={i} className="p-4 rounded-xl border-l-4 bg-red-50 border-red-200 flex items-start gap-3 shadow-sm">
          <AlertTriangle className="w-4 h-4 text-red-600 mt-0.5 flex-shrink-0" />
          <div className="flex-1">
            <p className="text-xs font-black text-red-700 leading-relaxed">{text}</p>
            {text.includes('MAPBIOMAS') && urls.length > 0 && (
              <a href={urls[0]} target="_blank" rel="noopener noreferrer" className="inline-flex items-center mt-2 text-[9px] font-black text-red-600 uppercase bg-white px-2 py-1 rounded border border-red-100 hover:bg-red-50 transition-colors">
                <ExternalLink className="w-3 h-3 mr-1" /> Ver Laudo Oficial
              </a>
            )}
          </div>
        </div>
      ))}
    </div>
  );
};
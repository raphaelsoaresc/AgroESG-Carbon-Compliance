import { FileText, AlertTriangle, Shield, ExternalLink, ArrowRight } from 'lucide-react';
import { AuditData } from '../types';
import LeadForm from '../LeadForm';

export const EvidenceSection = ({ data, onUnlock, carId }: { data: AuditData, onUnlock: () => void, carId: string }) => (
  <div className="bg-white rounded-3xl shadow-sm border border-slate-200 overflow-hidden relative">
    <div className="px-8 py-6 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
      <h3 className="text-sm font-bold text-slate-700 uppercase tracking-widest flex items-center gap-2">
        <FileText className="w-4 h-4 text-slate-400" /> Evidências de Auditoria
      </h3>
    </div>
    <div className="p-8 relative">
      <div className={`space-y-4 ${data.isCensored ? 'pb-40' : ''}`}>
        {data.evidenceList.map((text, index) => {
          const isRisk = 
            text.includes('🚨') || 
            text.includes('⚠️') || 
            text.includes('Restrição') || 
            text.includes('Invasão') || 
            text.includes('Inconformidade') ||
            text.includes('Supressão') || 
            text.includes('Risco') || 
            text.includes('Conflito') || 
            text.includes('BLOQUEADO') ||
            text.includes('Bloqueio') ||  
            text.includes('Atenção') || 
            text.includes('Incerteza') ||
            text.includes('Déficit') ||
            text.includes('Áreas Críticas');

          const isCensored = data.isCensored && index > 1;
          return (
            <div key={index} className={`group p-5 rounded-2xl border transition-all duration-300 flex flex-col gap-4 ${isRisk ? 'bg-red-50 border-red-100' : 'bg-slate-50 border-slate-100'} ${isCensored ? 'blur-[6px] opacity-30 pointer-events-none select-none' : 'hover:shadow-md hover:border-slate-300'}`}>
              <div className="flex items-start gap-4">
                <div className="mt-1">{isRisk ? <AlertTriangle className="w-5 h-5 text-red-500" /> : <Shield className="w-5 h-5 text-blue-500" />}</div>
                <p className={`text-sm font-semibold leading-relaxed ${isRisk ? 'text-red-900' : 'text-slate-700'}`}>{text}</p>
              </div>
              
              {text.includes('MapBiomas') && data.mapbiomasUrl && !isCensored && (
                <a
                  href={data.mapbiomasUrl.split('|')[0]} // Pega o primeiro link se houver vários
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center self-start px-4 py-2 rounded-xl bg-white border border-red-200 text-red-600 text-[10px] font-bold uppercase tracking-wider hover:bg-red-50 transition-colors shadow-sm mt-2"
                >
                  <ExternalLink className="w-3 h-3 mr-2" /> Acessar Laudo MapBiomas
                </a>
              )}
            </div>
          );
        })}
      </div>
      {data.isCensored && (
        <div className="absolute inset-x-0 bottom-0 h-80 bg-gradient-t from-white via-white/95 to-transparent z-20 flex flex-col items-center justify-end pb-10 px-8">
          <div className="w-full max-w-md bg-white p-8 rounded-[2.5rem] shadow-[0_20px_50px_rgba(0,0,0,0.1)] border border-slate-100 text-center">
            <h4 className="text-xl font-bold text-slate-900 mb-2">Relatório Completo Bloqueado</h4>
            <p className="text-slate-500 text-sm mb-8">Libere o acesso imediato a todos os dados de conformidade e passivos financeiros.</p>
            <button onClick={onUnlock} className="w-full bg-emerald-600 hover:bg-emerald-700 text-white font-bold py-4 rounded-2xl shadow-lg shadow-emerald-200 transition-all active:scale-[0.98] flex items-center justify-center gap-2 mb-4">
              Desbloquear Agora <ArrowRight className="w-4 h-4" />
            </button>
            <div className="mt-4"><LeadForm carId={carId} /></div>
          </div>
        </div>
      )}
    </div>
  </div>
);
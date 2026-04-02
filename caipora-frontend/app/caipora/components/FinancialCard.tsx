import { DollarSign, Lock } from 'lucide-react';
import { AuditData } from '../types';

export const FinancialCard = ({ data }: { data: AuditData }) => (
  <div className="bg-slate-900 rounded-3xl shadow-xl p-8 text-white relative overflow-hidden">
    <div className="relative z-10">
      <span className="text-[10px] font-bold uppercase tracking-[0.2em] text-white mb-4 block">Passivo Estimado Total</span>
      {data.isCensored ? (
        <div className="space-y-4">
          <div className="text-4xl font-bold tracking-tight blur-md select-none opacity-50">R$ 3.664.918</div>
          <div className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[10px] font-bold uppercase tracking-wider">
            <Lock className="w-3 h-3" /> Conteúdo Premium
          </div>
        </div>
      ) : (
        <>
          <div className="text-4xl font-bold tracking-tight mb-6 text-white">{data.liabilityTotal}</div>
          <div className="space-y-3 border-t border-white/10 pt-6">
            
            {/* Ambiental: Recuperação de solo (Desmatamento + RL + APP) */}
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400 font-medium">Ambiental (Recuperação: Desm./RL/APP)</span>
              <span className="text-xs font-bold text-red-500">{data.liabilityAmbientalTotal}</span>
            </div>
            
            {/* Embargos: Multas administrativas (Onde estão os seus R$ 4,6 milhões) */}
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400 font-medium">Embargos (Multas IBAMA/SEMA)</span>
              <span className="text-xs font-bold text-red-500">{data.liabilityEmbargo}</span>
            </div>

            {/* Áreas Protegidas: Multas por invasão de TI/UC */}
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400 font-medium">Áreas Protegidas (TI/UC)</span>
              <span className="text-xs font-bold text-red-500">{data.liabilityProtected}</span>
            </div>

            {/* Social: Multas trabalhistas */}
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400 font-medium">Social / Trabalhista</span>
              <span className="text-xs font-bold text-red-500">{data.liabilitySocial}</span>
            </div>
          </div>
        </>
      )}
    </div>
    <DollarSign className="absolute -right-4 -bottom-4 w-32 h-32 text-white/5 rotate-12" />
  </div>
);
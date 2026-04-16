import React from 'react';
import { DollarSign, Lock } from 'lucide-react';
import { AuditData } from '../types';
import { formatCurrency } from '../utils';

export const FinancialCard = ({ data }: { data: AuditData }) => {
  // Atalho para facilitar o acesso
  const liab = data.financial_liabilities;

  return (
    <div className="bg-slate-900 rounded-[2rem] shadow-2xl p-8 !text-white relative overflow-hidden border border-slate-800">
      <div className="relative z-10">
        <span className="text-[10px] font-black uppercase tracking-[0.2em] !text-slate-400 mb-4 block">
          Passivo Estimado Total
        </span>

        {data.isCensored ? (
          <div className="space-y-4">
            {/* Valor borrado para usuários não pagantes */}
            <div className="text-4xl font-black tracking-tight blur-md select-none opacity-50 !text-white">
              R$ 3.664.918
            </div>
            <div className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-emerald-500/10 border border-emerald-500/20 !text-emerald-400 text-[10px] font-black uppercase tracking-widest">
              <Lock className="w-3.5 h-3.5" /> Conteúdo Premium
            </div>
          </div>
        ) : (
          <>
            {/* Valor Total Real vindo do helper do useAudit */}
            <div className="text-4xl font-black tracking-tight mb-8 !text-white">
              {data.liabilityTotal}
            </div>

            <div className="space-y-4 border-t border-white/10 pt-6">
              
              {/* Ambiental: Soma de Desmatamento + RL + APP (Já vem formatado do useAudit) */}
              <div className="flex justify-between items-center">
                <span className="text-xs !text-slate-400 font-bold uppercase tracking-wider">Ambiental (Recuperação)</span>
                <span className="text-sm font-black !text-red-500">{data.liabilityAmbientalTotal}</span>
              </div>
              
              {/* Embargos: Multas administrativas */}
              <div className="flex justify-between items-center">
                <span className="text-xs !text-slate-400 font-bold uppercase tracking-wider">Embargos (Multas)</span>
                <span className="text-sm font-black !text-red-500">
                  {formatCurrency(liab?.liability_embargo_brl || 0)}
                </span>
              </div>

              {/* Áreas Protegidas: Invasão de TI/UC */}
              <div className="flex justify-between items-center">
                <span className="text-xs !text-slate-400 font-bold uppercase tracking-wider">Áreas Protegidas</span>
                <span className="text-sm font-black !text-red-500">
                  {formatCurrency(liab?.liability_protected_areas_brl || 0)}
                </span>
              </div>

              {/* Social: Multas trabalhistas / Lista Suja */}
              <div className="flex justify-between items-center">
                <span className="text-xs !text-slate-400 font-bold uppercase tracking-wider">Social / Trabalhista</span>
                <span className="text-sm font-black !text-red-500">
                  {formatCurrency(liab?.liability_social_brl || 0)}
                </span>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Ícone de fundo decorativo */}
      <DollarSign className="absolute -right-4 -bottom-4 w-32 h-32 !text-white/5 rotate-12" />
    </div>
  );
};
import React from "react";

export const MapLegend = () => {
  return (
    <div className="absolute bottom-6 left-6 z-[1000] bg-slate-900/90 backdrop-blur-md p-4 rounded-xl border border-slate-700 text-[10px] text-white shadow-2xl pointer-events-none">
      <h4 className="font-bold mb-2 text-slate-400 uppercase tracking-wider">Legenda de Compliance</h4>
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <div className="w-3 h-3 rounded-full bg-[#10b981] shadow-[0_0_8px_#10b981]"></div> <span>Elegível</span>
        </div>
        <div className="flex items-center gap-3">
          <div className="w-3 h-3 rounded-full bg-[#ef4444] shadow-[0_0_8px_#ef4444]"></div> <span>Bloqueio Direto</span>
        </div>
        <div className="flex items-center gap-3">
          <div className="w-3 h-3 rounded-full border-2 border-dashed border-[#f97316]"></div> <span>Vizinho de Risco</span>
        </div>
        <div className="flex items-center gap-3">
          <div className="w-3 h-3 rounded-full bg-[#d946ef] shadow-[0_0_8px_#d946ef]"></div> <span>Restrição EUDR</span>
        </div>
        <div className="flex items-center gap-3">
          <div className="w-3 h-3 rounded-full bg-[#facc15]"></div> <span>Desmatamento</span>
        </div>
      </div>
    </div>
  );
};
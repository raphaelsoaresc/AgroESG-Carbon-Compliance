import { ShieldCheck } from 'lucide-react';
import { determineStatusColor, translateConfidence, translateStatus } from '../../lib/audit-utils';

export function PropertyCard({ prop }: { prop: any }) {
  const color = determineStatusColor(prop.final_eligibility_status, prop.geospatial_confidence_level);
  const confidenceLabel = translateConfidence(prop.geospatial_confidence_level).split(':')[0];
  const translatedStatus = translateStatus(prop.final_eligibility_status);
  const biomeDisplay = prop.biome_name || prop.environmental_score?.biome_name || "SEM BIOMA";

  return (
    <div 
      onClick={() => window.location.href = `/caipora?car=${prop.property_id}`} 
      className="group cursor-pointer bg-white p-8 rounded-[3rem] border-2 border-slate-100 hover:border-emerald-500 hover:shadow-2xl transition-all duration-500 flex flex-col justify-between"
    >
      <div>
        <div className="flex justify-between items-center mb-8">
          <div className="flex flex-wrap gap-2">
            <div className={`px-3 py-1.5 rounded-xl text-[9px] font-black uppercase tracking-tighter border-2 ${
              color === 'red' ? 'bg-red-500 text-white border-red-400' : 
              color === 'orange' ? 'bg-orange-500 text-white border-orange-400' :
              color === 'blue' ? 'bg-blue-500 text-white border-blue-400' : 'bg-emerald-600 text-white border-emerald-500'
            }`}>
              {translatedStatus}
            </div>
            <div className="px-3 py-1.5 rounded-xl text-[9px] font-black uppercase tracking-tighter border-2 bg-slate-100 text-slate-500 border-slate-200 flex items-center gap-1">
              <ShieldCheck className={`w-3 h-3 ${prop.geospatial_confidence_level?.includes('HIGH') ? 'text-emerald-500' : 'text-amber-500'}`} />
              {confidenceLabel}
            </div>
          </div>
        </div>
        
        <h3 className="text-2xl font-mono font-black text-slate-900 break-all leading-none mb-2 group-hover:text-emerald-600 transition-colors">{prop.property_id}</h3>
        
        <div className="flex items-center gap-2 mb-4">
          <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">{biomeDisplay}</p>
          <span className="text-slate-200">•</span>
          <p className="text-[10px] font-black text-emerald-600 uppercase tracking-widest">{prop.producer_size_category}</p>
        </div>

        <div className="flex flex-wrap gap-1">
          {prop.property_identity_type?.includes('Assentamento') && <span className="bg-amber-100 text-amber-700 px-2 py-0.5 rounded-lg text-[8px] font-black uppercase">Assentamento</span>}
          {prop.property_identity_type?.includes('Terra Indígena') && <span className="bg-orange-100 text-orange-700 px-2 py-0.5 rounded-lg text-[8px] font-black uppercase">T.I.</span>}
          {prop.property_identity_type?.includes('Território Tradicional') && <span className="bg-blue-100 text-blue-700 px-2 py-0.5 rounded-lg text-[8px] font-black uppercase">Tradicional</span>}
          {prop.property_identity_type === 'Imóvel Rural Privado' && <span className="bg-slate-100 text-slate-600 px-2 py-0.5 rounded-lg text-[8px] font-black uppercase">Privado</span>}
        </div>
      </div>
      
      <div className="flex items-center justify-between pt-6 mt-8 border-t border-slate-50">
        <div className="text-[11px] font-black text-slate-500 uppercase tracking-tighter">{prop.city} / {prop.uf_origem}</div>
        <div className="text-slate-900 font-black text-sm">{(prop.area_ha || 0).toLocaleString('pt-BR')} <span className="text-[9px] text-slate-400">HA</span></div>
      </div>
    </div>
  );
}
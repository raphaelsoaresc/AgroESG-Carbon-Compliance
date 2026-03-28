import Image from 'next/image';
import Link from 'next/link';

export default function Footer() {
  return (
    <footer className="bg-white border-t border-slate-200 pt-24 pb-12 px-6 mt-20">
      <div className="max-w-7xl mx-auto">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-16 mb-16">
          <div className="space-y-8">
            <div className="relative w-72 h-32 md:w-[450px] md:h-48">
              <Image src="/logo-agrimarket.png" alt="Agri-Market" fill className="object-contain object-left" />
            </div>
            <p className="text-slate-500 text-xl leading-relaxed font-medium max-w-sm">
              Dados que plantam, tecnologia que protege.
            </p>
          </div>

          <div className="space-y-6">
            <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Contato Oficial</h5>
            <ul className="space-y-4 text-base text-slate-600">
              <li className="flex items-center gap-3">
                <span className="bg-slate-100 p-2 rounded-lg text-green-600 font-bold">✉</span>
                <a href="mailto:compliance@agrimarketintel.com" className="hover:text-green-600 transition-colors font-semibold">compliance@agrimarketintel.com</a>
              </li>
              <li className="flex items-center gap-3">
                <span className="bg-slate-100 p-2 rounded-lg text-green-600 font-bold">🌐</span>
                <a href="https://agrimarketintel.com" target="_blank" rel="noopener noreferrer" className="hover:text-green-600 transition-colors font-semibold">www.agrimarketintel.com</a>
              </li>
            </ul>
          </div>

          <div className="space-y-6">
            <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Data Sources</h5>
            <div className="flex flex-wrap gap-2">
              {['IBAMA', 'INCRA', 'MAPBIOMAS', 'INPE', 'MMA', 'EUDR-READY', 'CMN-5081'].map((source) => (
                <span key={source} className="bg-slate-900 text-white text-[10px] font-black px-3 py-1.5 rounded-md tracking-widest">{source}</span>
              ))}
            </div>
            <p className="text-[11px] text-slate-400 leading-relaxed italic font-medium mt-4">
              As análises geradas pela plataforma utilizam dados públicos e algoritmos proprietários de Risk Automation.
            </p>
          </div>
        </div>

        <div className="border-t border-slate-100 pt-10 flex flex-col md:flex-row justify-between items-center gap-8">
          <p className="text-sm text-slate-400 font-bold">© {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation.</p>
          <div className="flex flex-wrap justify-center gap-8 text-xs font-black uppercase tracking-widest text-slate-400">
            <Link href="/termos" className="hover:text-slate-900 transition-colors">Termos</Link>
            <Link href="/privacidade" className="hover:text-slate-900 transition-colors">Privacidade</Link>
            <a href="https://caipora-sentinela-api-534128993934.us-central1.run.app/docs" target="_blank" rel="noopener noreferrer" className="hover:text-slate-900 transition-colors border-b-2 border-green-500/30 pb-1">API OAS 3.1 (Swagger)</a>
          </div>
        </div>
      </div>
    </footer>
  );
}
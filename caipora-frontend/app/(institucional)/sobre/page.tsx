'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Institucional() {
  return (
    <div className="min-h-screen bg-white font-sans text-slate-900">
      {/* NAVBAR INSTITUCIONAL */}
      <nav className="sticky top-0 z-50 bg-white/80 backdrop-blur-md border-b border-slate-100 px-6 py-4">
        <div className="max-w-7xl mx-auto flex justify-between items-center">
          <div className="relative w-48 h-12">
            <Image src="/logo-agrimarket.jpg" alt="Agri-Market Logo" fill className="object-contain object-left" />
          </div>
          <div className="hidden md:flex gap-8 items-center font-bold text-sm uppercase tracking-widest text-slate-600">
            <a href="#solucoes" className="hover:text-green-600 transition-colors">Soluções</a>
            <a href="#tecnologia" className="hover:text-green-600 transition-colors">Tecnologia</a>
            <Link href="/" className="bg-slate-900 text-white px-6 py-3 rounded-full hover:bg-green-600 transition-all">
              Acessar Caipora
            </Link>
          </div>
        </div>
      </nav>

      {/* HERO SECTION */}
      <section className="relative pt-20 pb-32 px-6 overflow-hidden">
        <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
          <div className="space-y-8">
            <span className="inline-block bg-green-100 text-green-700 px-4 py-1 rounded-full text-xs font-black uppercase tracking-[0.2em]">
              Risk Automation & Intelligence
            </span>
            <h1 className="text-5xl md:text-7xl font-black text-slate-900 leading-[1.1]">
              Inteligência Geoespacial para <span className="text-green-600">Decisões de Alto Impacto.</span>
            </h1>
            <p className="text-xl text-slate-500 leading-relaxed max-w-xl">
              Transformamos dados complexos de satélite e registros fundiários em métricas financeiras acionáveis para o mercado de capitais e agronegócio global.
            </p>
            <div className="flex gap-4">
              <button className="bg-slate-900 text-white px-8 py-4 rounded-2xl font-bold text-lg hover:shadow-2xl hover:shadow-green-500/20 transition-all">
                Falar com um Especialista
              </button>
            </div>
          </div>
          <div className="relative h-[500px] rounded-[3rem] overflow-hidden shadow-2xl border-8 border-slate-100">
             <Image src="/logo-caipora.jpg" alt="Caipora Sentinela" fill className="object-cover" />
             <div className="absolute inset-0 bg-gradient-to-t from-slate-900/80 to-transparent flex items-end p-12">
                <p className="text-white font-mono text-sm">Caipora Sentinela v3.1.0: O motor de decisão por trás da Agri-Market.</p>
             </div>
          </div>
        </div>
      </section>

      {/* PILARES DE VALOR */}
      <section id="solucoes" className="bg-slate-50 py-24 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-20 space-y-4">
            <h2 className="text-4xl font-black uppercase tracking-tighter">Nossas Soluções</h2>
            <p className="text-slate-500 max-w-2xl mx-auto">Tecnologia proprietária para mitigar riscos socioambientais e garantir conformidade global.</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            {[
              {
                title: "Compliance EUDR",
                desc: "Análise automatizada de desmatamento pós-2020 para exportação imediata para a União Europeia.",
                icon: "🌍"
              },
              {
                title: "Risk Automation",
                desc: "Integração via API para esteiras de crédito agrícola com latência zero e precisão bitemporal.",
                icon: "⚡"
              },
              {
                title: "Market Intelligence",
                desc: "Mapeamento de polos logísticos e toxicidade de cadeias de suprimentos em tempo real.",
                icon: "📊"
              }
            ].map((item, i) => (
              <div key={i} className="bg-white p-10 rounded-[2.5rem] shadow-sm border border-slate-100 hover:border-green-500 transition-all group">
                <div className="text-4xl mb-6">{item.icon}</div>
                <h3 className="text-xl font-black mb-4 uppercase tracking-tight">{item.title}</h3>
                <p className="text-slate-500 leading-relaxed">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* TECNOLOGIA / DATA SOURCES */}
      <section id="tecnologia" className="py-24 px-6">
        <div className="max-w-5xl mx-auto text-center space-y-12">
          <h2 className="text-3xl font-black uppercase tracking-widest">Tecnologia de Ponta</h2>
          <p className="text-xl text-slate-600 leading-relaxed">
            Nossa plataforma processa diariamente terabytes de dados provenientes de constelações de satélites e bases governamentais, utilizando algoritmos de <strong>Risk Automation</strong> para eliminar o erro humano.
          </p>
          <div className="flex flex-wrap justify-center gap-8 opacity-50 grayscale hover:grayscale-0 transition-all">
             {/* Aqui você pode colocar logos pequenos das fontes de dados */}
             <span className="font-black text-2xl">IBAMA</span>
             <span className="font-black text-2xl">MAPBIOMAS</span>
             <span className="font-black text-2xl">ESA</span>
             <span className="font-black text-2xl">NASA</span>
             <span className="font-black text-2xl">INCRA</span>
          </div>
        </div>
      </section>

      {/* CTA FINAL */}
      <section className="bg-green-600 py-20 px-6 text-center">
        <div className="max-w-3xl mx-auto space-y-8">
          <h2 className="text-4xl md:text-5xl font-black text-white leading-tight">
            Pronto para automatizar seu compliance?
          </h2>
          <p className="text-green-100 text-xl">
            Junte-se às instituições que já utilizam a Agri-Market para proteger seus ativos.
          </p>
          <a href="mailto:compliance@agrimarketintel.com" className="inline-block bg-slate-900 text-white px-10 py-5 rounded-2xl font-black text-xl shadow-2xl hover:scale-105 transition-all">
            Solicitar Demonstração
          </a>
        </div>
      </section>

      {/* FOOTER (Reutilizando o estilo do dashboard) */}
      <footer className="bg-white border-t border-slate-200 pt-20 pb-12 px-6">
        <div className="max-w-6xl mx-auto text-center space-y-8">
          <div className="relative w-64 h-24 mx-auto">
            <Image src="/logo-agrimarket.jpg" alt="Agri-Market" fill className="object-contain" />
          </div>
          <p className="text-slate-400 text-sm font-bold uppercase tracking-widest">
            © {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation.
          </p>
        </div>
      </footer>
    </div>
  );
}
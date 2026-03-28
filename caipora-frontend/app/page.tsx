'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Institucional() {
  // Dados extraídos do seu banco para a tabela de estados
  const stateData =[
    { uf: 'PA', props: '378.265', area: '99M', falsos: '52.182', eudr: '52.779', cmn: '21.224', esg: '2.328', adj: '53.817' },
    { uf: 'MT', props: '211.772', area: '100M', falsos: '28.023', eudr: '11.501', cmn: '13.276', esg: '1.346', adj: '38.849' },
    { uf: 'RO', props: '188.623', area: '23M', falsos: '21.337', eudr: '16.282', cmn: '13.061', esg: '1.094', adj: '33.202' },
    { uf: 'AM', props: '98.080', area: '93M', falsos: '15.299', eudr: '13.064', cmn: '8.644', esg: '1.433', adj: '12.096' },
  ];

  return (
    <div className="min-h-screen bg-white font-sans text-slate-900 flex flex-col">
      
      {/* NAVBAR INSTITUCIONAL */}
      <nav className="sticky top-0 z-50 bg-white/90 backdrop-blur-md border-b border-slate-100 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
          <Link href="/" className="relative w-[280px] h-16 md:w-[550px] md:h-44 transition-transform hover:scale-105">
            <Image 
              src="/logo-agrimarket.png" 
              alt="Agri-Market Intelligence Logo" 
              fill 
              className="object-contain object-center md:object-left" 
              priority 
            />
          </Link>

          <div className="flex flex-wrap justify-center gap-4 md:gap-10 items-center font-bold text-[10px] md:text-sm uppercase tracking-widest text-slate-600">
            <a href="#solucoes" className="hover:text-green-600 transition-colors">Soluções</a>
            <a href="#impacto" className="hover:text-green-600 transition-colors">Resultados</a>
            <Link href="/caipora" className="bg-slate-900 text-white px-6 py-3 md:px-8 md:py-4 rounded-full hover:bg-green-600 transition-all shadow-xl">
              Acessar Caipora
            </Link>
          </div>
        </div>
      </nav>

      {/* HERO SECTION */}
      <section className="relative pt-20 pb-32 px-6 overflow-hidden">
        <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">
          <div className="space-y-10">
            <span className="inline-block bg-green-100 text-green-700 px-6 py-2 rounded-full text-xs font-black uppercase tracking-[0.3em]">
              Risk Automation & Intelligence
            </span>
            <h1 className="text-6xl md:text-8xl font-black text-slate-900 leading-[1.05] tracking-tighter">
              Inteligência Geoespacial para <span className="text-green-600">Decisões de Alto Impacto.</span>
            </h1>
            <p className="text-xl md:text-2xl text-slate-500 leading-relaxed max-w-xl font-medium">
              Transformamos dados complexos de satélite e registros fundiários em métricas financeiras acionáveis para o mercado de capitais e agronegócio global.
            </p>
            <div className="flex flex-col sm:flex-row gap-6">
              <a href="mailto:compliance@agrimarketintel.com" className="bg-slate-900 text-white px-10 py-5 rounded-2xl font-bold text-xl text-center hover:shadow-2xl hover:shadow-green-500/30 transition-all active:scale-95">
                Falar com um Especialista
              </a>
              <Link href="/caipora" className="border-2 border-slate-200 px-10 py-5 rounded-2xl font-bold text-xl text-center hover:bg-slate-50 transition-all active:scale-95">
                Testar Demo Caipora
              </Link>
            </div>
          </div>

          <div className="relative h-[600px] rounded-[4rem] overflow-hidden shadow-[0_0_100px_rgba(0,0,0,0.1)] border-[12px] border-slate-50">
             <Image src="/logo-caipora.jpg" alt="Caipora Sentinela Engine" fill className="object-cover" />
             <div className="absolute inset-0 bg-gradient-to-t from-slate-900/90 via-slate-900/20 to-transparent flex items-end p-12">
                <div className="space-y-2">
                  <p className="text-green-400 font-black uppercase tracking-widest text-xs">Powered by Agri-Market</p>
                  <p className="text-white font-mono text-lg">Caipora Sentinela v3.1.0: O motor de decisão bitemporal líder do mercado.</p>
                </div>
             </div>
          </div>
        </div>
      </section>

      {/* SEÇÃO SOLUÇÕES */}
      <section id="solucoes" className="bg-slate-50 py-32 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-24 space-y-6">
            <h2 className="text-5xl font-black uppercase tracking-tighter text-slate-900">Nossas Soluções</h2>
            <p className="text-xl text-slate-500 max-w-3xl mx-auto font-medium">
              Tecnologia proprietária para mitigar riscos socioambientais e garantir conformidade com as normas globais mais rigorosas.
            </p>
          </div>

          {/* Alterado para md:grid-cols-2 para formar um quadrado perfeito (2x2) com os 4 itens */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
            {[
              { 
                title: "Compliance EUDR", 
                desc: "Análise automatizada de desmatamento pós-2020 para garantir elegibilidade de exportação para a União Europeia.", 
                icon: "🇪🇺" 
              },
              { 
                title: "Risk Automation", 
                desc: "Integração via API para esteiras de crédito agrícola com latência zero e precisão cirúrgica bitemporal.", 
                icon: "⚙️" 
              },
              { 
                title: "Market Intelligence", 
                desc: "Mapeamento de polos logísticos e monitoramento Dinâmico de Cadeias de Suprimentos com Sincronização de Alta Frequência (D+7 a D+15).", 
                icon: "📈" 
              },
              { 
                title: "Data-as-a-Service", 
                desc: "Acesso a datasets em GeoParquet com +870k registros. Sincronização dinâmica a cada 7-15 dias para máxima fidelidade geoespacial.", 
                icon: "📦" 
              }
            ].map((item, i) => (
              <div key={i} className="bg-white p-12 rounded-[3rem] shadow-sm border border-slate-100 hover:border-green-500 hover:shadow-2xl hover:shadow-green-500/10 transition-all group">
                <div className="text-5xl mb-8">{item.icon}</div>
                <h3 className="text-2xl font-black mb-6 uppercase tracking-tight text-slate-900">{item.title}</h3>
                <p className="text-slate-500 text-lg leading-relaxed">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* NOVA SEÇÃO: DADOS E IMPACTO */}
      <section id="impacto" className="bg-slate-900 py-32 px-6 text-white">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-20 space-y-6">
            <span className="text-green-400 font-black uppercase tracking-[0.3em] text-sm">Nossos Resultados</span>
            <h2 className="text-5xl font-black uppercase tracking-tighter text-white">Impacto em Escala Continental</h2>
            <p className="text-xl text-slate-400 max-w-3xl mx-auto font-medium">
              Volume de dados processados e riscos mitigados pelo nosso motor de inteligência em todo o território nacional.
            </p>
          </div>

          {/* KPIs - Linha Brasil (Total) */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
            {[
              { label: "Área Monitorada", value: "314 Mi", suffix: "ha", color: "text-white" },
              { label: "Propriedades Analisadas", value: "876.740", suffix: "", color: "text-white" },
              { label: "Violações EUDR Barradas", value: "93.626", suffix: "", color: "text-red-400" },
              { label: "Falsos Positivos Mitigados", value: "116.841", suffix: "", color: "text-green-400" }
            ].map((kpi, i) => (
              <div key={i} className="bg-slate-800/50 border border-slate-700 p-8 rounded-3xl hover:bg-slate-800 transition-colors">
                <p className="text-slate-400 text-sm font-bold uppercase tracking-widest mb-4">{kpi.label}</p>
                <div className="flex items-baseline gap-2">
                  <span className={`text-5xl font-black tracking-tighter ${kpi.color}`}>{kpi.value}</span>
                  <span className="text-xl font-bold text-slate-500">{kpi.suffix}</span>
                </div>
              </div>
            ))}
          </div>

          {/* Tabela de Estados */}
          <div className="bg-slate-800/30 border border-slate-700 rounded-3xl overflow-hidden">
            <div className="p-8 border-b border-slate-700 flex flex-col md:flex-row justify-between items-center gap-4">
              <h3 className="text-2xl font-bold">Detalhamento por Estado (Amazônia Legal)</h3>
              <div className="flex gap-4 text-sm font-bold text-slate-400">
                <span className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-red-400"></span> Bloqueios CMN 5.081: 56.205</span>
                <span className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-orange-400"></span> Conflitos ESG: 6.201</span>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-left border-collapse whitespace-nowrap">
                <thead>
                  <tr className="bg-slate-800/50 text-slate-400 text-xs uppercase tracking-widest">
                    <th className="p-6 font-bold">Estado</th>
                    <th className="p-6 font-bold">Propriedades</th>
                    <th className="p-6 font-bold">Área (ha)</th>
                    <th className="p-6 font-bold">Falsos Positivos</th>
                    <th className="p-6 font-bold">Violações EUDR</th>
                    <th className="p-6 font-bold">Bloqueios CMN</th>
                    <th className="p-6 font-bold">Risco Adjacência</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-700/50">
                  {stateData.map((row, i) => (
                    <tr key={i} className="hover:bg-slate-800/50 transition-colors">
                      <td className="p-6 font-black text-lg">{row.uf}</td>
                      <td className="p-6 font-mono text-slate-300">{row.props}</td>
                      <td className="p-6 font-mono text-slate-300">{row.area}</td>
                      <td className="p-6 font-mono text-green-400">{row.falsos}</td>
                      <td className="p-6 font-mono text-red-400">{row.eudr}</td>
                      <td className="p-6 font-mono text-orange-400">{row.cmn}</td>
                      <td className="p-6 font-mono text-slate-300">{row.adj}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </section>

      {/* SEÇÃO TECNOLOGIA */}
      <section id="tecnologia" className="py-32 px-6 bg-white">
        <div className="max-w-5xl mx-auto text-center space-y-16">
          <h2 className="text-4xl font-black uppercase tracking-[0.2em] text-slate-900">Tecnologia de Ponta</h2>
          <p className="text-2xl text-slate-600 leading-relaxed font-light">
            Nossa plataforma processa diariamente terabytes de dados provenientes de constelações de satélites e bases governamentais, utilizando algoritmos de <strong>Risk Automation</strong> para eliminar o erro humano e garantir a segurança jurídica dos seus ativos.
          </p>
          <div className="flex flex-wrap justify-center gap-12 opacity-40 grayscale hover:grayscale-0 transition-all duration-500">
             {['IBAMA', 'MAPBIOMAS', 'ESA', 'NASA', 'INCRA', 'INPE'].map(source => (
               <span key={source} className="font-black text-3xl tracking-tighter">{source}</span>
             ))}
          </div>
        </div>
      </section>

      {/* CTA FINAL */}
      <section className="bg-green-600 py-24 px-6 text-center">
        <div className="max-w-4xl mx-auto space-y-10">
          <h2 className="text-5xl md:text-7xl font-black text-white leading-tight tracking-tighter">
            Pronto para automatizar seu compliance?
          </h2>
          <p className="text-green-100 text-2xl font-medium">
            Antecipe-se à EUDR e à CMN 5.081. Adote a infraestrutura de inteligência geoespacial construída para as instituições que não podem errar.
          </p>
          <a href="mailto:compliance@agrimarketintel.com" className="inline-block bg-slate-900 text-white px-12 py-6 rounded-3xl font-black text-2xl shadow-2xl hover:scale-105 transition-all active:scale-95">
            Solicitar Demonstração Enterprise
          </a>
        </div>
      </section>

     {/* RODAPÉ MESTRE */}
      <footer className="bg-white border-t border-slate-200 pt-24 pb-12 px-6 mt-20">
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-16 mb-16">
            
            <div className="space-y-8">
              <div className="relative w-72 h-32 md:w-[450px] md:h-48">
                <Image 
                  src="/logo-agrimarket.png" 
                  alt="Agri-Market Intelligence & Risk Automation" 
                  fill 
                  className="object-contain object-left"
                />
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
                  <a href="mailto:compliance@agrimarketintel.com" className="hover:text-green-600 transition-colors font-semibold">
                    compliance@agrimarketintel.com
                  </a>
                </li>
                <li className="flex items-center gap-3">
                  <span className="bg-slate-100 p-2 rounded-lg text-green-600 font-bold">🌐</span>
                  <a href="https://agrimarketintel.com" target="_blank" rel="noopener noreferrer" className="hover:text-green-600 transition-colors font-semibold">
                    www.agrimarketintel.com
                  </a>
                </li>
              </ul>
            </div>

            <div className="space-y-6">
              <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Data Sources</h5>
              <div className="flex flex-wrap gap-2">
                {['IBAMA', 'INCRA', 'MAPBIOMAS', 'INPE', 'MMA', 'EUDR-READY', 'CMN-5081'].map((source) => (
                  <span key={source} className="bg-slate-900 text-white text-[10px] font-black px-3 py-1.5 rounded-md tracking-widest">
                    {source}
                  </span>
                ))}
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed italic font-medium mt-4">
                As análises geradas pela plataforma utilizam dados públicos e algoritmos proprietários de Risk Automation.
              </p>
            </div>
          </div>

          <div className="border-t border-slate-100 pt-10 flex flex-col md:flex-row justify-between items-center gap-8">
            <p className="text-sm text-slate-400 font-bold">
              © {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation.
            </p>
            <div className="flex flex-wrap justify-center gap-8 text-xs font-black uppercase tracking-widest text-slate-400">
              <Link href="/termos" className="hover:text-slate-900 transition-colors">Termos</Link>
              <Link href="/privacidade" className="hover:text-slate-900 transition-colors">Privacidade</Link>
              <a 
                href="https://caipora-sentinela-api-534128993934.us-central1.run.app/docs" 
                target="_blank" 
                rel="noopener noreferrer" 
                className="hover:text-slate-900 transition-colors border-b-2 border-green-500/30 pb-1"
              >
                API OAS 3.1 (Swagger)
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}
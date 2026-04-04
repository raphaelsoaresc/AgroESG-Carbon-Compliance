'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Institucional() {
  const stateData =[
    { uf: 'PA', props: '378.265', area: '99M', falsos: '52.182', eudr: '52.779', cmn: '21.224', esg: '2.328', adj: '53.817' },
    { uf: 'MT', props: '211.772', area: '100M', falsos: '28.023', eudr: '11.501', cmn: '13.276', esg: '1.346', adj: '38.849' },
    { uf: 'RO', props: '188.623', area: '23M', falsos: '21.337', eudr: '16.282', cmn: '13.061', esg: '1.094', adj: '33.202' },
    { uf: 'AM', props: '98.080', area: '93M', falsos: '15.299', eudr: '13.064', cmn: '8.644', esg: '1.433', adj: '12.096' },
  ];

  return (
    <div className="min-h-screen bg-white font-sans text-slate-900 flex flex-col">
      
      {/* NAVBAR */}
      <nav className="sticky top-0 z-50 bg-white/90 backdrop-blur-md border-b border-slate-100 px-6 py-4">
        <div className="max-w-7xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4">
          <Link href="/" className="relative w-[280px] h-16 md:w-[550px] md:h-44 transition-transform hover:scale-105">
            <Image src="/logo-agrimarket.png" alt="Agri-Market Logo" fill className="object-contain" priority />
          </Link>

          <div className="flex flex-wrap justify-center gap-4 md:gap-10 items-center font-bold text-[10px] md:text-sm uppercase tracking-widest text-slate-600">
            <a href="#solucoes" className="hover:text-green-600 transition-colors">Soluções</a>
            <a href="#planos" className="hover:text-green-600 transition-colors font-black">Planos SaaS</a>
            <a href="#art" className="hover:text-green-600 transition-colors">Laudos ART</a>
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
              24+ Data Layers | CREA PR-237151/D
            </span>
            <h1 className="text-6xl md:text-8xl font-black text-slate-900 leading-[1.05] tracking-tighter">
              Auditoria Geoespacial com <span className="text-green-600">Rigor Pericial.</span>
            </h1>
            <p className="text-xl md:text-2xl text-slate-500 leading-relaxed max-w-xl font-medium">
              O único motor de decisão bitemporal que integra análise de adjacência, mitigação por barreiras hidrográficas e cálculo de passivo financeiro real.
            </p>
            <div className="flex flex-col sm:flex-row gap-6">
              <a href="#planos" className="bg-slate-900 text-white px-10 py-5 rounded-2xl font-bold text-xl text-center hover:bg-slate-800 transition-all active:scale-95">
                Ver Planos SaaS
              </a>
              <a href="#art" className="border-2 border-green-600 text-green-600 px-10 py-5 rounded-2xl font-bold text-xl text-center hover:bg-green-50 transition-all active:scale-95">
                Solicitar Laudo ART
              </a>
            </div>
          </div>

          <div className="relative h-[600px] rounded-[4rem] overflow-hidden shadow-[0_0_100px_rgba(0,0,0,0.1)] border-[12px] border-slate-50">
             <Image src="/logo-caipora.jpg" alt="Caipora Sentinela" fill className="object-cover" />
             <div className="absolute inset-0 bg-gradient-to-t from-slate-900/90 via-slate-900/20 to-transparent flex items-end p-12">
                <div className="space-y-2">
                  <p className="text-green-400 font-black uppercase tracking-widest text-xs">Intelligence Engine</p>
                  <p className="text-white font-mono text-lg">Caipora Sentinela: Auditoria de 870k imóveis em segundos.</p>
                </div>
             </div>
          </div>
        </div>
      </section>

      {/* SEÇÃO SOLUÇÕES */}
      <section id="solucoes" className="bg-slate-50 py-32 px-6">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-24 space-y-6">
            <h2 className="text-5xl font-black uppercase tracking-tighter text-slate-900">O Motor de Decisão</h2>
            <p className="text-xl text-slate-500 max-w-3xl mx-auto font-medium">
              Tecnologia proprietária que elimina o "custo de confiança" no agronegócio através de 24 camadas de dados integradas.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
            {[
              { 
                title: "Adjacency Intelligence", 
                desc: "Algoritmo exclusivo que identifica riscos em vizinhos, com mitigação automática via barreiras hidrográficas (ANA), eliminando bloqueios indevidos por 'contágio'.", 
                icon: "🌊" 
              },
              { 
                title: "Geospatial Forensic", 
                desc: "Recortes exatos de invasão e cálculo automático de passivo financeiro (multas estimadas BRL) para análise de LGD e risco de crédito.", 
                icon: "⚖️" 
              },
              { 
                title: "Compliance EUDR & CMN", 
                desc: "Monitoramento bitemporal de desmatamento pós-2008 e pós-2020. Pronto para as exigências da União Europeia e Resolução CMN 5.081.", 
                icon: "🇪🇺" 
              },
              { 
                title: "Data-as-a-Service (DaaS)", 
                desc: "Acesso a datasets em GeoParquet com +870k registros. Sincronização dinâmica a cada 7-15 dias para máxima fidelidade geoespacial.", 
                icon: "📦" 
              }
            ].map((item, i) => (
              <div key={i} className="bg-white p-12 rounded-[3rem] shadow-sm border border-slate-100 hover:border-green-500 hover:shadow-2xl transition-all group">
                <div className="text-5xl mb-8">{item.icon}</div>
                <h3 className="text-2xl font-black mb-6 uppercase tracking-tight text-slate-900">{item.title}</h3>
                <p className="text-slate-500 text-lg leading-relaxed">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* SEÇÃO PLANOS SAAS */}
      <section id="planos" className="py-32 px-6 bg-white">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-20 space-y-4">
            <h2 className="text-5xl font-black uppercase tracking-tighter">Planos de Acesso</h2>
            <p className="text-xl text-slate-500 font-medium">Escolha o nível de profundidade da sua auditoria.</p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-8 max-w-5xl mx-auto">
            
            {/* PLANO FREE */}
            <div className="border-2 border-slate-100 p-12 rounded-[3rem] flex flex-col space-y-8 hover:border-slate-300 transition-all">
              <div className="space-y-2">
                <h3 className="text-3xl font-black uppercase">Plano Explorer</h3>
                <p className="text-slate-500 font-medium text-lg">Para prospecção e consultas rápidas.</p>
              </div>
              <div className="text-5xl font-black">R$ 0<span className="text-lg text-slate-400">/mês</span></div>
              <ul className="space-y-4 flex-grow">
                <li className="flex items-center gap-3 font-bold text-slate-700">
                  <span className="text-green-500 text-xl">✓</span> Acesso ao <strong>Catálogo Sentinela</strong>
                </li>
                <li className="flex items-center gap-3 font-bold text-slate-700">
                  <span className="text-green-500 text-xl">✓</span> Filtros por Bioma, UF e Porte
                </li>
                <li className="flex items-center gap-3 font-bold text-slate-700">
                  <span className="text-green-500 text-xl">✓</span> 3 Auditorias detalhadas/mês
                </li>
                <li className="flex items-center gap-3 font-bold text-slate-400 line-through">
                  <span className="text-red-400 text-xl">✕</span> Cálculo de Passivo Estimado (BRL)
                </li>
                <li className="flex items-center gap-3 font-bold text-slate-400 line-through">
                  <span className="text-red-400 text-xl">✕</span> Visualização de mapas e recortes
                </li>
              </ul>
              <Link href="/caipora" className="w-full py-5 rounded-2xl border-2 border-slate-900 text-center font-black text-xl hover:bg-slate-900 hover:text-white transition-all">
                Começar Agora
              </Link>
            </div>

            {/* PLANO PRO */}
            <div className="bg-slate-900 p-12 rounded-[3rem] flex flex-col space-y-8 relative overflow-hidden shadow-2xl shadow-green-500/20 border-2 border-green-500">
              <div className="absolute top-8 right-8 bg-green-500 text-slate-900 px-4 py-1 rounded-full text-xs font-black uppercase tracking-widest">Recomendado</div>
              <div className="space-y-2">
                <h3 className="text-3xl font-black uppercase text-white">Plano PRO</h3>
                <p className="text-slate-400 font-medium text-lg">A solução definitiva para Tradings e Fiagros.</p>
              </div>
              <div className="text-5xl font-black text-white">R$ 4.000<span className="text-lg text-slate-500">/mês</span></div>
              
              <div className="bg-green-500/10 border border-green-500/20 p-4 rounded-2xl text-center">
                <p className="text-green-400 font-black text-sm uppercase tracking-tighter">
                  ⭐ INCLUSO: 3 Créditos de Laudo ART/mês
                </p>
              </div>

              <ul className="space-y-4 flex-grow text-white">
                <li className="flex items-center gap-3 font-bold">
                  <span className="text-green-400 text-xl">✓</span> <strong>50 Auditorias Completas/mês</strong>
                </li>
                <li className="flex items-center gap-3 font-bold">
                  <span className="text-green-400 text-xl">✓</span> Cálculo de Passivo Total (Ambiental/Social)
                </li>
                <li className="flex items-center gap-3 font-bold">
                  <span className="text-green-400 text-xl">✓</span> Mapas Interativos com Camadas GIS
                </li>
                <li className="flex items-center gap-3 font-bold">
                  <span className="text-green-400 text-xl">✓</span> Indicadores EUDR-Ready e CMN 5.081
                </li>
                <li className="flex items-center gap-3 font-bold text-green-400">
                  <span className="text-xl">✓</span> 3 Laudos Periciais assinados (CREA)
                </li>
              </ul>
              <a href="mailto:compliance@agrimarketintel.com?subject=Assinatura Plano PRO" className="w-full bg-green-600 text-white text-center py-5 rounded-2xl font-black text-xl hover:bg-green-500 transition-all">
                Assinar Agora
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* SEÇÃO ART - SERVIÇO CONSULTIVO */}
      <section id="art" className="py-32 px-6 bg-slate-50">
        <div className="max-w-7xl mx-auto bg-slate-900 rounded-[4rem] p-12 md:p-24 overflow-hidden relative">
          <div className="relative z-10 grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">
            <div className="space-y-8">
              <h2 className="text-5xl md:text-6xl font-black text-white leading-tight">
                Auditoria Técnica com <span className="text-green-400">Validade Jurídica.</span>
              </h2>
              <p className="text-slate-400 text-xl leading-relaxed font-medium">
                Transformamos os dados do sistema em evidência legal. Emitimos laudos periciais assinados com ART (Anotação de Responsabilidade Técnica) por profissional habilitado.
              </p>
              <div className="bg-white/5 p-6 rounded-2xl border border-white/10 space-y-2">
                <p className="text-white font-black uppercase tracking-widest text-sm">Responsável Técnico:</p>
                <p className="text-green-400 font-mono text-lg">Registro Crea nº PR-237151/D</p>
                <p className="text-green-400 font-mono text-lg">Visto CREA-AM nº 172392095-9</p>
              </div>
            </div>

            <div className="bg-white p-10 rounded-[3rem] space-y-8 text-center shadow-2xl">
              <h3 className="text-2xl font-black text-slate-900 uppercase tracking-widest">Laudo sob Demanda</h3>
              <p className="text-slate-500 font-medium italic">"A segurança de um perito, com a velocidade da tecnologia."</p>
              
              <div className="space-y-4 text-left">
                <div className="flex items-center gap-4 text-slate-700 font-bold">
                  <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center text-slate-900">1</div>
                  <span>Solicite informando o CAR</span>
                </div>
                <div className="flex items-center gap-4 text-slate-700 font-bold">
                  <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center text-slate-900">2</div>
                  <span>Pagamento via Fatura/Pix</span>
                </div>
                <div className="flex items-center gap-4 text-slate-700 font-bold">
                  <div className="w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center text-slate-900">3</div>
                  <span>Receba no e-mail em 7 dias</span>
                </div>
              </div>

              <div className="pt-6 border-t border-slate-100">
                <a href="mailto:compliance@agrimarketintel.com?subject=Solicitação de Laudo ART" className="block w-full bg-slate-900 text-white text-center py-5 rounded-2xl font-black text-xl hover:bg-green-600 transition-all">
                  Solicitar Orçamento
                </a>
                <p className="text-[10px] text-slate-400 mt-4 uppercase font-black tracking-widest">Serviço em conformidade com a Resolução Confea 313/86</p>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* IMPACTO E TABELA */}
      <section id="impacto" className="bg-slate-900 py-32 px-6 text-white">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-20 space-y-6">
            <span className="text-green-400 font-black uppercase tracking-[0.3em] text-sm">Impacto em Escala</span>
            <h2 className="text-5xl font-black uppercase tracking-tighter text-white">314 Milhões de Hectares</h2>
            <p className="text-xl text-slate-400 max-w-3xl mx-auto font-medium">
              Volume de dados processados e riscos mitigados pelo nosso motor de inteligência em tempo real.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
            {[
              { label: "Área Monitorada", value: "314 Mi", suffix: "ha", color: "text-white" },
              { label: "Propriedades Analisadas", value: "876.740", suffix: "", color: "text-white" },
              { label: "Violações EUDR Barradas", value: "93.626", suffix: "", color: "text-red-400" },
              { label: "Falsos Positivos Mitigados*", value: "116.841", suffix: "", color: "text-green-400" }
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
          <p className="text-xs text-slate-500 mb-10 italic">*Falsos positivos mitigados via análise de identidade e barreiras hidrográficas.</p>

          {/* Tabela de Estados */}
          <div className="bg-slate-800/30 border border-slate-700 rounded-3xl overflow-hidden">
            <div className="p-8 border-b border-slate-700 flex flex-col md:flex-row justify-between items-center gap-4">
              <h3 className="text-2xl font-bold">Detalhamento por Estado (Amazônia Legal)</h3>
              <div className="flex gap-4 text-sm font-bold text-slate-400">
                <span className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-red-400"></span> Bloqueios CMN: 56.205</span>
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

      {/* RODAPÉ */}
      <footer className="bg-white border-t border-slate-200 pt-24 pb-12 px-6 mt-20">
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-16 mb-16">
            <div className="space-y-8">
              <div className="relative w-72 h-32 md:w-[450px] md:h-48">
                <Image src="/logo-agrimarket.png" alt="Agri-Market Logo" fill className="object-contain object-left" />
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
                <li className="flex items-center gap-3 font-semibold">🌐 www.agrimarketintel.com</li>
              </ul>
            </div>

            <div className="space-y-6">
              <h5 className="font-black text-slate-900 uppercase tracking-[0.2em] text-sm border-l-4 border-green-500 pl-4">Data Sources & RT</h5>
              <div className="flex flex-wrap gap-2">
                {['IBAMA', 'INCRA', 'MAPBIOMAS', 'INPE', 'EUDR-READY', 'CMN-5081'].map((source) => (
                  <span key={source} className="bg-slate-900 text-white text-[10px] font-black px-3 py-1.5 rounded-md tracking-widest">
                    {source}
                  </span>
                ))}
              </div>
              <p className="text-[11px] text-slate-400 leading-relaxed italic font-medium mt-4">
                Análises técnicas assinadas por Tecnólogo em Agronegócio. CREA PR-237151/D | Visto CREA-AM 172392095-9.
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
              <a href="https://caipora-sentinela-api-534128993934.us-central1.run.app/docs" target="_blank" rel="noopener noreferrer" className="hover:text-slate-900 transition-colors border-b-2 border-green-500/30 pb-1">
                API OAS 3.1 (Swagger)
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  );
}
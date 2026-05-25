'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Institucional() {
  // Dados atualizados com todos os 27 estados
  const stateData = [
    { uf: 'BA', props: '1.278.858', area: '38.0', falsos: '3.633', eudr: '35.578', cmn: '0', adj: '364.091' },
    { uf: 'MG', props: '1.160.867', area: '55.6', falsos: '1.241', eudr: '25.604', cmn: '0', adj: '437.279' },
    { uf: 'RS', props: '667.589', area: '24.9', falsos: '1.132', eudr: '8.972', cmn: '0', adj: '214.582' },
    { uf: 'PR', props: '552.963', area: '19.2', falsos: '3.396', eudr: '5.485', cmn: '0', adj: '214.312' },
    { uf: 'SP', props: '465.623', area: '23.8', falsos: '1.796', eudr: '609', cmn: '0', adj: '329.900' },
    { uf: 'PE', props: '431.711', area: '8.0', falsos: '1.586', eudr: '16.049', cmn: '0', adj: '191.066' },
    { uf: 'SC', props: '428.105', area: '8.7', falsos: '1.054', eudr: '3.720', cmn: '0', adj: '131.191' },
    { uf: 'MA', props: '409.665', area: '32.8', falsos: '3.520', eudr: '36.455', cmn: '909', adj: '270.631' },
    { uf: 'CE', props: '405.132', area: '12.1', falsos: '1.242', eudr: '26.864', cmn: '0', adj: '259.549' },
    { uf: 'PA', props: '381.583', area: '58.3', falsos: '4.380', eudr: '58.205', cmn: '21.649', adj: '336.297' },
    { uf: 'PI', props: '331.914', area: '20.7', falsos: '536', eudr: '23.208', cmn: '0', adj: '181.405' },
    { uf: 'GO', props: '237.866', area: '33.9', falsos: '1.575', eudr: '9.838', cmn: '0', adj: '189.981' },
    { uf: 'PB', props: '215.927', area: '4.8', falsos: '1.780', eudr: '9.336', cmn: '0', adj: '94.132' },
    { uf: 'MT', props: '213.370', area: '77.9', falsos: '52.476', eudr: '12.920', cmn: '22.980', adj: '200.728' },
    { uf: 'RO', props: '189.679', area: '15.7', falsos: '782', eudr: '18.225', cmn: '13.242', adj: '159.102' },
    { uf: 'AL', props: '142.814', area: '2.6', falsos: '2.151', eudr: '2.155', cmn: '0', adj: '54.079' },
    { uf: 'ES', props: '128.130', area: '4.0', falsos: '16', eudr: '856', cmn: '0', adj: '38.819' },
    { uf: 'SE', props: '122.024', area: '2.0', falsos: '314', eudr: '2.692', cmn: '0', adj: '49.022' },
    { uf: 'RN', props: '116.034', area: '4.5', falsos: '3.788', eudr: '3.742', cmn: '0', adj: '45.567' },
    { uf: 'TO', props: '109.034', area: '25.4', falsos: '519', eudr: '12.590', cmn: '335', adj: '97.485' },
    { uf: 'AM', props: '98.218', area: '21.6', falsos: '6.257', eudr: '14.366', cmn: '8.416', adj: '72.258' },
    { uf: 'MS', props: '88.318', area: '30.0', falsos: '1.253', eudr: '3.467', cmn: '0', adj: '54.879' },
    { uf: 'RJ', props: '68.099', area: '3.2', falsos: '93', eudr: '378', cmn: '0', adj: '26.089' },
    { uf: 'AC', props: '60.650', area: '6.6', falsos: '224', eudr: '20.997', cmn: '8.903', adj: '56.834' },
    { uf: 'RR', props: '29.793', area: '8.5', falsos: '550', eudr: '6.748', cmn: '3.247', adj: '27.757' },
    { uf: 'DF', props: '20.965', area: '0.8', falsos: '6', eudr: '96', cmn: '0', adj: '20.876' },
    { uf: 'AP', props: '17.400', area: '3.4', falsos: '60', eudr: '684', cmn: '630', adj: '12.210' },
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
            <a href="#art" className="hover:text-green-600 transition-colors">Especialidades ART</a>
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
            <span className="inline-flex items-center gap-2 bg-green-100 text-green-700 px-6 py-2 rounded-full text-xs font-black uppercase tracking-[0.3em]">
              <span className="w-2 h-2 bg-green-600 rounded-full animate-pulse"></span>
              Habilitado para Perícia Judicial | CREA PR-237151/D
            </span>
            <h1 className="text-6xl md:text-8xl font-black text-slate-900 leading-[1.05] tracking-tighter">
              Auditoria Geoespacial com <span className="text-green-600">Rigor Pericial.</span>
            </h1>
            <p className="text-xl md:text-2xl text-slate-500 leading-relaxed max-w-xl font-medium">
              O único motor de decisão bitemporal que integra prova de nexo causal, análise de adjacência e evidências técnicas com validade jurídica.
            </p>
            <div className="flex flex-col sm:flex-row gap-6">
              <Link href="/caipora" className="bg-slate-900 text-white px-10 py-5 rounded-2xl font-bold text-xl text-center hover:bg-slate-800 transition-all active:scale-95">
                Acessar Plataforma
              </Link>
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
                  <p className="text-white font-mono text-lg">Caipora Sentinela: Auditoria de 8.3M imóveis em segundos.</p>
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
                desc: "Algoritmo exclusivo que identifica riscos em vizinhos e conexões por estradas, com mitigação automática via barreiras hidrográficas (ANA), eliminando bloqueios indevidos por 'contágio' geográfico.", 
                icon: "🌊" 
              },
              { 
                title: "Geospatial Forensic", 
                desc: "Identificação de Nexo Causal Logístico para prova de dolo (intersecção estrada/pista x crime), com recortes exatos de invasão e cálculo automático de passivo financeiro (BRL) para análise de LGD e risco de crédito.", 
                icon: "⚖️" 
              },
              { 
                title: "Compliance EUDR & CMN", 
                desc: "Monitoramento bitemporal de desmatamento pós-2008 e pós-2020. Pronto para as exigências da União Europeia e Resolução CMN 5.081.", 
                icon: "🇪🇺" 
              },
              { 
                title: "Data-as-a-Service (DaaS)", 
                desc: "Acesso a datasets em GeoParquet com +8.3M registros. Sincronização dinâmica a cada 7-15 dias para máxima fidelidade geoespacial.", 
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

      {/* SEÇÃO ART - ESPECIALIDADES PERICIAIS */}
      <section id="art" className="py-32 px-6 bg-white">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-16 space-y-4">
            <h2 className="text-5xl font-black uppercase tracking-tighter text-slate-900">
              Especialidades com <span className="text-green-600">Fé Pública</span>
            </h2>
            <p className="text-slate-500 text-xl font-medium max-w-3xl mx-auto">
              Emissão de ART (Anotação de Responsabilidade Técnica) conforme Resolução CONFEA 313/86 para fins judiciais e corporativos.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mb-16">
            <div className="p-10 border border-slate-100 rounded-[3rem] bg-slate-50 hover:shadow-2xl transition-all group">
              <div className="w-14 h-14 bg-slate-900 text-white rounded-2xl flex items-center justify-center mb-8 font-black text-xl group-hover:bg-green-600 transition-colors">01</div>
              <h4 className="text-2xl font-black mb-6 uppercase tracking-tight">Perícia Judicial & Forense</h4>
              <ul className="text-slate-600 space-y-4 text-base font-bold">
                <li className="flex items-start gap-2"><span>•</span> Laudos de Confrontação (SIGEF/CAR)</li>
                <li className="flex items-start gap-2"><span>•</span> Nexo Causal de Desmatamento</li>
                <li className="flex items-start gap-2"><span>•</span> Prova Técnica em Invasões Territoriais</li>
                <li className="flex items-start gap-2"><span>•</span> Assistência Técnica em Processos</li>
              </ul>
            </div>

            <div className="p-10 border border-slate-100 rounded-[3rem] bg-slate-50 hover:shadow-2xl transition-all group">
              <div className="w-14 h-14 bg-slate-900 text-white rounded-2xl flex items-center justify-center mb-8 font-black text-xl group-hover:bg-green-600 transition-colors">02</div>
              <h4 className="text-2xl font-black mb-6 uppercase tracking-tight">Compliance & Auditoria ESG</h4>
              <ul className="text-slate-600 space-y-4 text-base font-bold">
                <li className="flex items-start gap-2"><span>•</span> Auditoria para Resolução CMN 5.081</li>
                <li className="flex items-start gap-2"><span>•</span> Verificação de Conformidade EUDR</li>
                <li className="flex items-start gap-2"><span>•</span> Relatórios de Risco para FIAGROS</li>
                <li className="flex items-start gap-2"><span>•</span> Monitoramento de Embargos</li>
              </ul>
            </div>

            <div className="p-10 border border-slate-100 rounded-[3rem] bg-slate-50 hover:shadow-2xl transition-all group">
              <div className="w-14 h-14 bg-slate-900 text-white rounded-2xl flex items-center justify-center mb-8 font-black text-xl group-hover:bg-green-600 transition-colors">03</div>
              <h4 className="text-2xl font-black mb-6 uppercase tracking-tight">Gestão de Ativos & Passivos</h4>
              <ul className="text-slate-600 space-y-4 text-base font-bold">
                <li className="flex items-start gap-2"><span>•</span> Cálculo de Passivo Financeiro Ambiental</li>
                <li className="flex items-start gap-2"><span>•</span> Análise de Viabilidade de Garantias</li>
                <li className="flex items-start gap-2"><span>•</span> Levantamento de Custos de Regularização</li>
                <li className="flex items-start gap-2"><span>•</span> Avaliação de Riscos Operacionais</li>
              </ul>
            </div>
          </div>

          <div className="bg-slate-900 rounded-[3rem] p-12 flex flex-col md:flex-row items-center justify-between gap-8">
            <div className="space-y-2">
              <p className="text-green-400 font-mono text-sm uppercase tracking-widest">Responsável Técnico Habilitado</p>
              <p className="text-white text-2xl font-black">Registro Crea nº PR-237151/D</p>
              <p className="text-slate-400 text-sm">Vistos ativos: SP nº 5071832461 | AM nº 172392095-9</p>
            </div>
            <a href="mailto:compliance@agrimarketintel.com?subject=Solicitação de Laudo ART" className="bg-white text-slate-900 px-12 py-5 rounded-2xl font-black text-xl hover:bg-green-500 hover:text-white transition-all">
              Solicitar Orçamento de Laudo
            </a>
          </div>
        </div>
      </section>

      {/* IMPACTO E TABELA */}
      <section id="impacto" className="bg-slate-900 py-32 px-6 text-white">
        <div className="max-w-7xl mx-auto">
          <div className="text-center mb-20 space-y-6">
            <span className="text-green-400 font-black uppercase tracking-[0.3em] text-sm">Impacto em Escala</span>
            <h2 className="text-5xl font-black uppercase tracking-tighter text-white">547 Milhões de Hectares</h2>
            <p className="text-xl text-slate-400 max-w-3xl mx-auto font-medium">
              Volume de dados processados e riscos mitigados pelo nosso motor de inteligência em tempo real em todo o território nacional.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
            {[
              { label: "Área Monitorada", value: "547 Mi", suffix: "ha", color: "text-white" },
              { label: "Propriedades Analisadas", value: "8.372.331", suffix: "", color: "text-white" },
              { label: "Violações EUDR Barradas", value: "359.839", suffix: "", color: "text-red-400" },
              { label: "Falsos Positivos Mitigados*", value: "95.360", suffix: "", color: "text-green-400" }
            ].map((kpi, i) => (
              <div key={i} className="bg-slate-800/50 border border-slate-700 p-8 rounded-3xl hover:bg-slate-800 transition-colors">
                <p className="text-slate-400 text-sm font-bold uppercase tracking-widest mb-4">{kpi.label}</p>
                <div className="flex items-baseline gap-2">
                  <span className={`text-4xl font-black tracking-tighter ${kpi.color}`}>{kpi.value}</span>
                  <span className="text-xl font-bold text-slate-500">{kpi.suffix}</span>
                </div>
              </div>
            ))}
          </div>

          {/* Tabela de Estados */}
          <div className="bg-slate-800/30 border border-slate-700 rounded-3xl overflow-hidden">
            <div className="p-8 border-b border-slate-700 flex flex-col md:flex-row justify-between items-center gap-4">
              <h3 className="text-2xl font-bold">Detalhamento por Estado (Nacional)</h3>
              <div className="flex gap-4 text-sm font-bold text-slate-400">
                <span className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-red-400"></span> Bloqueios CMN: 80.311</span>
                <span className="flex items-center gap-2"><span className="w-3 h-3 rounded-full bg-orange-400"></span> Conflitos ESG: 1.130.973</span>
              </div>
            </div>
            <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
              <table className="w-full text-left border-collapse whitespace-nowrap">
                <thead className="sticky top-0 bg-slate-900 z-10">
                  <tr className="bg-slate-800 text-slate-400 text-xs uppercase tracking-widest">
                    <th className="p-6 font-bold">Estado</th>
                    <th className="p-6 font-bold">Propriedades</th>
                    <th className="p-6 font-bold">Área (Mi ha)</th>
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

          {/* METODOLOGIA */}
          <div className="pt-48 mt-20 space-y-16 border-t border-slate-800">
            <div className="text-center space-y-6">
              <h3 className="text-4xl md:text-5xl font-black uppercase tracking-tighter">Metodologia de Auditoria Forense Digital e Compliance Socioambiental</h3>
              <p className="text-slate-400 text-xl max-w-4xl mx-auto font-medium">
                O sistema utiliza uma arquitetura de Inteligência de Dados Geográfica para realizar a auditoria automatizada de ativos agrários. A metodologia é dividida em quatro pilares fundamentais:
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
              {/* Pilar 1 */}
              <div className="bg-slate-800/40 border border-slate-700 p-12 rounded-[4rem] space-y-6">
                <h4 className="text-2xl font-bold text-green-400">1. Ingestão de Fontes Oficiais (Transparência e Fé Pública)</h4>
                <p className="text-slate-300 text-sm leading-relaxed">
                  A análise não se baseia em opiniões, mas em dados brutos extraídos diretamente das bases de dados do Governo Federal e Estadual, garantindo a integridade da prova:
                </p>
                <ul className="text-slate-400 text-xs space-y-2">
                  <li><strong>• Dados Fundiários:</strong> SIGEF (INCRA) e CAR (Serviço Florestal Brasileiro).</li>
                  <li><strong>• Dados de Fiscalização:</strong> Histórico de embargos do IBAMA, ICMBio e Secretarias Estaduais (SEMA).</li>
                  <li><strong>• Dados Sociais:</strong> Cadastro de Empregadores do MTE (Lista Suja do Trabalho Escravo).</li>
                  <li><strong>• Dados Geográficos:</strong> Malhas do IBGE, ANA (Hidrografia) e MapBiomas (Alertas de Desmatamento).</li>
                </ul>
              </div>

              {/* Pilar 2 */}
              <div className="bg-slate-800/40 border border-slate-700 p-12 rounded-[4rem] space-y-6">
                <h4 className="text-2xl font-bold text-green-400">2. Higienização e Geoprocessamento (Precisão Técnica)</h4>
                <p className="text-slate-300 text-sm leading-relaxed">
                  Os dados brutos passam por um rigoroso processo de tratamento:
                </p>
                <ul className="text-slate-400 text-xs space-y-2">
                  <li><strong>• Deduplicação Forense:</strong> Garantia de que apenas a versão mais recente e válida de cada documento (CAR/SIGEF) seja analisada.</li>
                  <li><strong>• Padronização Espacial:</strong> Conversão de coordenadas e polígonos para um formato geográfico unificado, permitindo o cruzamento exato de malhas.</li>
                  <li><strong>• Trava de Sanidade:</strong> Identificação de fraudes de área ou inconsistências documentais (áreas declaradas x desenho geográfico).</li>
                </ul>
              </div>

              {/* Pilar 3 */}
              <div className="bg-slate-800/40 border border-slate-700 p-12 rounded-[4rem] space-y-6">
                <h4 className="text-2xl font-bold text-green-400">3. Análise Bitemporal e Regras de Negócio (Conformidade Legal)</h4>
                <p className="text-slate-300 text-sm leading-relaxed">
                  O motor aplica automaticamente as legislações vigentes:
                </p>
                <ul className="text-slate-400 text-xs space-y-2">
                  <li><strong>• Marco Temporal (Código Florestal):</strong> Diferenciação de supressão de vegetação pré e pós 22 de julho de 2008.</li>
                  <li><strong>• Compliance Bancário (CMN 5.081):</strong> Verificação automática de restrições para concessão de crédito rural em biomas protegidos.</li>
                  <li><strong>• Regras Internacionais (EUDR):</strong> Auditoria de desmatamento pós-2020 para fins de exportação para a União Europeia.</li>
                </ul>
              </div>

              {/* Pilar 4 */}
              <div className="bg-slate-800/40 border border-slate-700 p-12 rounded-[4rem] space-y-6">
                <h4 className="text-2xl font-bold text-green-400">4. Inteligência de Vizinhança e Nexo Causal (Análise de Risco Avançada)</h4>
                <p className="text-slate-300 text-sm leading-relaxed">
                  Diferente de uma análise comum, esta metodologia investiga o contexto da propriedade:
                </p>
                <ul className="text-slate-400 text-xs space-y-2">
                  <li><strong>• Risco de Adjacência:</strong> Identifica se a fazenda vizinha possui crimes ambientais e se há vetores logísticos (estradas ou rios) que conectam as duas.</li>
                  <li><strong>• Nexo Causal Logístico:</strong> Verifica se invasões em TIs ou UCs são servidas por infraestrutura interna (pistas de pouso ou estradas).</li>
                  <li><strong>• Validação por Sensores:</strong> Uso de dados de radar e satélite (SRTM/NDVI) para confirmar a topografia e o uso real do solo.</li>
                </ul>
              </div>
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
                Análises técnicas assinadas por Tecnólogo em Agronegócio. CREA PR-237151/D | Visto CREA-SP 5071832461 | Visto CREA-AM 172392095-9. Em conformidade com a Resolução CONFEA 313/86.
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
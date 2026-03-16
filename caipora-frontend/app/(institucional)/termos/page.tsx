'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Termos() {
  return (
    <div className="min-h-screen bg-slate-50 font-sans text-slate-900">
      <header className="bg-slate-900 py-6 px-6 border-b-4 border-green-500">
        <div className="max-w-4xl mx-auto flex justify-between items-center">
          <Link href="/" className="flex items-center gap-4">
            <div className="w-12 h-12 relative rounded-lg overflow-hidden bg-white">
              <Image src="/logo-caipora.jpg" alt="Caipora" fill className="object-cover" />
            </div>
            <span className="text-white font-black tracking-tighter text-xl">Caipora <span className="text-green-400">Sentinela</span></span>
          </Link>
          <Link href="/" className="text-slate-400 hover:text-white text-sm font-bold transition-colors">
            ← Voltar ao Dashboard
          </Link>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-6 py-16">
        <p className="text-sm font-bold text-slate-500 mb-2 uppercase tracking-widest">Última Atualização: Março de 2026</p>
        <h1 className="text-4xl font-black mb-8 border-b-8 border-green-500 inline-block">Termos de Uso</h1>
        
        <div className="prose prose-slate max-w-none space-y-8 text-slate-700 leading-relaxed">
          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">1. Aceitação dos Termos</h2>
            <p>Ao acessar e utilizar a plataforma e a API do <strong>Caipora Sentinela</strong>, desenvolvida pela <strong>Agri-Market Intelligence & Risk Automation</strong> ("Agri-Market"), você concorda expressamente com estes Termos de Uso. Se você não concorda com qualquer parte destes termos, não deve utilizar nossos serviços.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">2. Natureza dos Serviços</h2>
            <p>O Caipora Sentinela é uma ferramenta B2B de automação de inteligência geoespacial e análise de risco ESG. Nossa plataforma agrega, cruza e processa dados de fontes públicas e oficiais (incluindo, mas não se limitando a: SICAR, IBAMA, MapBiomas, MTE, FUNAI, INCRA) combinados com imagens de satélite.</p>
            <div className="bg-orange-50 border-l-4 border-orange-500 p-4 mt-4 text-orange-800 rounded-r-lg">
              <strong>Importante:</strong> Os laudos, vereditos e estimativas de passivos financeiros gerados pela plataforma são de caráter consultivo e analítico. Eles <strong>não substituem</strong> pareceres jurídicos, auditorias de campo presenciais ou certidões oficiais emitidas por órgãos governamentais.
            </div>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">3. Isenção e Limitação de Responsabilidade</h2>
            <p>A Agri-Market fornece a plataforma "no estado em que se encontra" (as-is). Embora apliquemos algoritmos rigorosos de validação e processamento de dados, não garantimos a exatidão absoluta dos dados originais fornecidos por terceiros ou órgãos governamentais.</p>
            <p>A Agri-Market <strong>não se responsabiliza</strong> por perdas financeiras, bloqueios comerciais, quebras de safra, multas ou decisões de concessão/negação de crédito e seguros tomadas exclusivamente com base nas informações do nosso painel ou API. O risco fiduciário e a decisão final são de inteira responsabilidade do usuário (instituição financeira, trading ou empresa estruturadora).</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">4. Uso Aceitável e Propriedade Intelectual</h2>
            <p>Todo o código, algoritmos de cruzamento bitemporal, design de interface, documentação da API e inteligência de dados são propriedade exclusiva da Agri-Market. É estritamente proibido:</p>
            <ul className="list-disc pl-6 mt-4 space-y-2">
              <li>Realizar engenharia reversa, descompilar ou tentar extrair o código-fonte da API ou da interface.</li>
              <li>Utilizar <em>web scraping</em>, robôs ou métodos automatizados não autorizados para extrair dados do painel web. O acesso em massa deve ser feito exclusivamente mediante contratação da nossa API oficial.</li>
              <li>Revender, licenciar ou distribuir os relatórios gerados sem autorização comercial prévia e expressa (formato White-Label).</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">5. Suspensão de Acesso</h2>
            <p>Reservamo-nos o direito de bloquear, suspender ou cancelar contas e chaves de API que violem estes termos, tentem fraudar os limites de consultas estabelecidos no plano contratado, ou que tentem sobrecarregar nossos servidores intencionalmente.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">6. Contato e Foro</h2>
            <p>Para dúvidas técnicas, comerciais ou jurídicas sobre estes termos, a comunicação deve ser feita através do e-mail oficial: <a href="mailto:compliance@agrimarketintel.com" className="text-green-600 font-bold hover:underline">compliance@agrimarketintel.com</a>.</p>
          </section>
        </div>
      </main>

      <footer className="bg-white border-t border-slate-200 py-8 text-center text-slate-400 text-xs font-bold uppercase tracking-widest">
        © {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation
      </footer>
    </div>
  );
}
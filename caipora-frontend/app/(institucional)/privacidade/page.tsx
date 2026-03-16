'use client';
import Link from 'next/link';
import Image from 'next/image';

export default function Privacidade() {
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
        <h1 className="text-4xl font-black mb-8 border-b-8 border-green-500 inline-block">Política de Privacidade</h1>
        
        <div className="prose prose-slate max-w-none space-y-8 text-slate-700 leading-relaxed">
          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">1. Nosso Compromisso com a Privacidade</h2>
            <p>A <strong>Agri-Market Intelligence & Risk Automation</strong> está comprometida com a proteção dos seus dados corporativos e em conformidade com as legislações vigentes de proteção de dados, incluindo a LGPD (Lei Geral de Proteção de Dados - Lei nº 13.709/2018).</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">2. Dados que Coletamos</h2>
            <p>Nossa plataforma e API operam estritamente no modelo B2B (Business-to-Business). Coletamos apenas os dados necessários para a prestação e segurança do serviço:</p>
            <ul className="list-disc pl-6 mt-4 space-y-2">
              <li><strong>Dados de Cadastro e Contato:</strong> Nome, e-mail corporativo, cargo, empresa e telefone, fornecidos voluntariamente na solicitação de contato, demonstrações ou criação de conta.</li>
              <li><strong>Logs de API e Uso:</strong> Registramos o volume de requisições, endereços IP, chaves de API utilizadas e os parâmetros de busca (ex: números de CAR ou polígonos consultados) para fins de tarifação (billing), segurança e melhoria da eficiência do nosso algoritmo.</li>
              <li><strong>Informações Públicas:</strong> Os dados espaciais, ambientais e fundiários (CAR, embargos, alertas de desmatamento) processados pela nossa máquina são estritamente de domínio público e governamental, não constituindo dados pessoais protegidos por sigilo.</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">3. Como Usamos os Seus Dados</h2>
            <p>As informações corporativas coletadas são utilizadas exclusivamente para:</p>
            <ul className="list-disc pl-6 mt-4 space-y-2">
              <li>Fornecer, operar e manter a API e o painel web Caipora Sentinela.</li>
              <li>Processar faturamento e gerenciar o limite de requisições do seu plano contratado (<em>rate limiting</em>).</li>
              <li>Entrar em contato para suporte técnico, envio de relatórios solicitados, laudos ou propostas comerciais.</li>
              <li>Detectar, prevenir e resolver problemas técnicos, tentativas de fraude ou violações de segurança cibernética.</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">4. Compartilhamento de Dados</h2>
            <p>Nós <strong>não vendemos, alugamos ou comercializamos</strong> os seus dados de contato corporativo ou históricos de pesquisa para terceiros. O compartilhamento ocorre apenas com fornecedores de infraestrutura de nuvem e serviços de segurança estritamente necessários para manter a plataforma no ar (como serviços de hospedagem em nuvem), que também operam sob rigorosos padrões globais de conformidade e privacidade.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">5. Segurança da Informação</h2>
            <p>Utilizamos criptografia padrão da indústria (TLS/SSL) para todo o tráfego de dados entre o seu sistema e a nossa API. Implementamos controles de acesso rigorosos aos nossos bancos de dados para proteger o seu histórico de consultas e métricas financeiras trafegadas.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">6. Seus Direitos</h2>
            <p>Conforme estabelecido pela LGPD, você tem o direito de solicitar o acesso, a correção, a atualização ou a exclusão completa dos seus dados de contato corporativo da nossa base comercial a qualquer momento.</p>
          </section>

          <section>
            <h2 className="text-2xl font-bold text-slate-900 mb-4">7. Contato do Encarregado de Dados (DPO)</h2>
            <p>Para exercer seus direitos de privacidade ou tirar dúvidas sobre esta política e o tratamento de dados no Caipora Sentinela, fale diretamente com nossa equipe de conformidade através do e-mail: <a href="mailto:compliance@agrimarketintel.com" className="text-green-600 font-bold hover:underline">compliance@agrimarketintel.com</a>.</p>
          </section>
        </div>
      </main>

      <footer className="bg-white border-t border-slate-200 py-8 text-center text-slate-400 text-xs font-bold uppercase tracking-widest">
        © {new Date().getFullYear()} Agri-Market Intelligence & Risk Automation
      </footer>
    </div>
  );
}
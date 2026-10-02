# 🖥️ Caipora Sentinela - Frontend (Next.js)

Este diretório contém a interface de consumo visual do **Caipora Sentinela**. O objetivo desta camada é servir a visualização de relatórios, dashboards de elegibilidade territorial e indicadores analíticos de risco socioambiental para o Agronegócio.

A interface consome diretamente os endpoints de compliance expostos pelo serviço backend em FastAPI (localizado na pasta `/services`).

## 🚀 Stack Técnica do Frontend

*   **Frameworks:** Next.js
*   **Linguagem:** TypeScript / Python
*   **Estilização:** Tailwind CSS
*   **Componentes Visuais:** Integração com mapas interativos utilizando dados do IBAMA/INPE e alertas satelitais do MapBiomas.

## 🌟 O que esta camada entrega

*   **Dashboard de Compliance:** Painel para visualização de vereditos de elegibilidade territorial com filtros de risco socioambiental.
*   **Visualização de Mapas:** Carregamento dinâmico de mapas cruzando o Marco Temporal (pós-2008), sobreposição de Terras Indígenas/Quilombolas e Áreas de Preservação Permanente (APP).
*   **Emissão de Vereditos:** Interface para geração e exportação automatizada de relatórios de risco em PDF.

## ⚙️ Como Rodar a Interface

Como o projeto utiliza um ambiente isolado e reprodutível via **Nix (Devenv)** e gerenciamento de dependências via **uv**, o frontend deve ser iniciado preferencialmente através do ecossistema central do repositório.

### Através do Ambiente Global (Raiz do Projeto)

1. Volte para a raiz do repositório, entre no ambiente isolado e garanta que as dependências estejam sincronizadas:
   ```bash
   devenv shell
   uv sync
   ```

2. Suba todos os serviços orquestrados (o que incluirá esta interface e a API do backend):
   ```bash
   devenv up
   ```

## 📁 Integração no Repositório

Este diretório funciona de forma integrada com as demais pastas do ecossistema Caipora Sentinela:
*   `../services/` – Backend em FastAPI que serve os dados para esta interface.
*   `../utils/` – Helpers e integrações GIS utilizados para manipulação de geometrias espaciais.

## 🔐 Segurança e Variáveis de Ambiente

*   Nunca comite arquivos de ambiente locais (`.env.local` ou `.env`) neste diretório.
*   Toda integração com endpoints locais ou de staging deve ser configurada via variáveis de ambiente injetadas pelo `devenv`.

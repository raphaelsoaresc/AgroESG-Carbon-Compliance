# 🌿 Caipora Sentinela | Motor de Compliance Geoespacial para o Agronegócio

> **Status:** Orquestração com Airflow + Cosmos • Regras com dbt + BigQuery • Monitoramento MapBiomas • Dashboard em Streamlit • API em FastAPI

O Caipora Sentinela é uma plataforma de engenharia de dados geoespaciais para validar critérios ESG em crédito rural e mercado de carbono. O projeto integra dados oficiais (CAR, SIGEF, IBAMA, MTE, ANA) com alertas satelitais do MapBiomas para gerar um veredito de elegibilidade territorial.

## 🌟 O que este projeto entrega

* Avaliação automatizada de risco socioambiental para propriedades rurais em Mato Grosso.
* Uso do MapBiomas Alertas como fonte de verdade para desmatamento pós-2008.
* Cruzamento espacial entre registros administrativos e evidências remotas.
* Testes de conformidade e integridade de dados implementados com dbt.
* Interface de consumo via API e dashboard.

## 🎯 Problema resolvido

O projeto resolve o desafio de verificar se uma propriedade rural está apta a receber crédito ou ser elegível ao mercado de carbono, com base em:

* Alertas de desmatamento legal / ilegal.
* Embargos administrativos do IBAMA.
* Risco social ligado à Lista Suja do MTE.
* Sobreposição com Terras Indígenas e Quilombolas.
* Áreas de Preservação Permanente (APP).
* Indícios de degradação seletiva e topografia crítica.
* Risco de contaminação por áreas vizinhas.

## 🧠 Arquitetura resumida

O fluxo segue um padrão medallion: ingestão bronze, transformação silver e produto gold.

1. **Ingestão (Bronze)**
   * Airflow para ingestão de shapefiles, CSVs e bases governamentais.
   * DuckDB Spatial para pré-processar geometrias e gerar Parquet.
   * Google Earth Engine para métricas de elevação e vegetação.

2. **Transformação (Silver)**
   * dbt padroniza esquemas, valida geometrias e aplica limpeza.
   * Regras de Marco Temporal removem alertas anteriores a 22/07/2008.

3. **Inteligência (Gold)**
   * BigQuery executa joins espaciais em larga escala.
   * Modelos geram tabelas de risco e elegibilidade final.

4. **Consumo**
   * FastAPI expõe endpoints de compliance.
   * Frontend em Next.js/Streamlit serve visualização e relatórios.

## 🧰 Stack Técnica

* Python 3.11
* Apache Airflow
* Astronomer Cosmos
* DuckDB Spatial
* Google BigQuery + GCS
* dbt Core
* FastAPI + Uvicorn
* Streamlit / Next.js
* Google Earth Engine
* Nix via Devenv + uv

## 🚀 Como rodar

### 1. Preparar o ambiente

Mantenha `.env` local e fora do Git. Se não existir um `.env.example`, crie o arquivo manualmente.

```bash
cp .env.example .env
```

> Se `.env.example` não existir, crie seu `.env` com as variáveis necessárias.

### 2. Entrar no ambiente isolado

```bash
devenv shell
uv sync
```

### 3. Subir serviços

```bash
devenv up
```

### 4. Iniciar Airflow

```bash
start-airflow
```

Acesse o Airflow em: `http://localhost:8080`

### 5. Executar dbt

```bash
dbt run --select +fct_compliance_risk
dbt test
```

## 📁 Estrutura do repositório

* `airflow/` – DAGs e configurações de orquestração
* `agro_credit_transform/` – dbt project, modelos, seeds e testes
* `caipora-frontend/` – frontend Next.js / Streamlit
* `services/` – backend FastAPI
* `utils/` – integrações GIS e helpers
* `config/` – arquivos de configuração locais
* `data/` – dados brutos e staging

## 🧪 Testes e governança de dados

O sistema inclui testes de qualidade e regras de negócio em dbt para assegurar:

* validade das geometrias
* resultados de elegibilidade
* integridade de indicadores ambientais
* consistência de dados de risco

## 🚩 Diferenciais do projeto

* Uso do MapBiomas Alertas como fonte de decisão jurídica.
* Arquitetura com dbt + BigQuery para escalabilidade espacial.
* Engenharia de dados orientada a compliance e auditoria.
* Integração de risco social e ambiental em um único motor de decisão.

## 🔐 Segurança

* Não comite arquivos de ambiente (`.env`, `.env.local`).
* Não comite chaves ou arquivos de credenciais (`config/*.json`).
* Use variáveis de ambiente para dados sensíveis.

## ⚖️ Licença

Projeto licenciado sob MIT. Veja o arquivo [LICENSE](LICENSE).

**Autor:** Raphael Soares

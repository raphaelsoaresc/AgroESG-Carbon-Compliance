# 🌿 Caipora Sentinela | Motor de Compliance Geoespacial para o Agronegócio

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://caipora.agrimarketintel.com/)

> **Status:** ✅ Orquestração (Airflow + Cosmos) | 🧠 Motor de Regras (dbt + BigQuery) | 🛰️ **Monitoramento MapBiomas (Alertas)** | 📊 Dashboard (Streamlit) | 🚀 API (FastAPI)

O **Caipora Sentinela** é um motor completo de **Geospatial Data & Analytics Engineering** focado na validação rigorosa de critérios ESG para a originação de crédito rural e mercado de carbono. Com foco estratégico no estado do **Mato Grosso (MT)**, a *engine* processa dados de mais de **211.000 propriedades**.

O diferencial do projeto é a integração do **MapBiomas Alertas** como o "Grande Juiz" (Veredito Final), cruzando dados administrativos (CAR/SIGEF) com evidências físicas de desmatamento para garantir tolerância zero ao desmatamento ilegal pós-2008.

## 🚀 Evolução do Projeto: Da Experimentação à Produção

O projeto nasceu de uma necessidade de automatizar análises que antes eram manuais e fragmentadas. A arquitetura evoluiu significativamente:

*   **Fase 1 (Legado):** O fluxo dependia de **Jupyter Notebooks** e scripts de scraping instáveis. Usando **Python (pandas/geopandas)**, as transformações esbarravam em limites de memória local e os dados iam para o **DBeaver/PostGIS** para cruzamentos espaciais manuais.
*   **Fase 2 (Atual - O Nascimento do Caipora):** Para ganhar escala e governança, o sistema foi refatorado para uma arquitetura de **Data Lakehouse**. Substituímos os notebooks por DAGs no **Airflow**, o PostGIS local pelo **BigQuery** (para processamento massivo distribuído) e o dbt passou a gerenciar a linhagem e os testes de conformidade.

## 🎯 O Problema de Negócio

Para garantir a integridade dos créditos de carbono, combater o *Greenwashing* e mitigar riscos na cadeia de suprimentos, o motor do Caipora Sentinela automatiza a resposta para:

1.  **O Veredito Final (MapBiomas):** A propriedade possui alertas de desmatamento validados pelo MapBiomas após o Marco Temporal (Julho/2008)? Se sim, o bloqueio é imediato (`NOT ELIGIBLE`), independente de outras certidões administrativas.
2.  **Rastreabilidade Territorial da "Lista Suja" (Social):** Como localizar fazendas citadas apenas por nome na lista do MTE? Implementamos algoritmos de **Fuzzy Matching** e normalização de strings (remoção de abreviações como "FAZ.", "STO.") para criar uma ponte probabilística ("High/Medium Confidence") entre o PDF do governo e os polígonos reais do SIGEF.
3.  **Proteção de Territórios Sensíveis:** Sobreposição com Terras Indígenas (FUNAI) ou Quilombolas (INCRA).
4.  **Embargos Administrativos:** Invasão de áreas embargadas pelo IBAMA.
5.  **Proteção Hídrica (APPs):** Monitoramento cirúrgico de Áreas de Preservação Permanente. Não olhamos apenas a fazenda, mas recortamos digitalmente as margens dos rios para análise isolada.
6.  **Ground Truth & Desmatamento Seletivo:** A propriedade esconde crimes ambientais?
    *   **Topografia:** Bloqueio de áreas com declividade > 45° (Topo de Morro).
    *   **Anomalia de APP:** Detecção automática do padrão *"Fazenda Verde, Rio Morto"* (NDVI Geral > 0.6 vs. NDVI APP < 0.4), identificando degradação oculta que médias gerais não pegam.
7.  **Risco de Contaminação (Adjacency Risk):** A propriedade é vizinha de uma área desmatada ou embargada, sugerindo "lavagem" de commodities?

## 🏗 Arquitetura e Fluxo de Dados (Medallion Architecture)

O projeto processa dados de múltiplas fontes governamentais (SIGEF, CAR, IBAMA, MTE, ANA) e satelitais através de um fluxo estruturado:

*   **1. Ingestão Inteligente (Bronze):** DAGs no Airflow com **Content-Based Routing** identificam automaticamente arquivos ZIP contendo Shapefiles (Geometria) ou CSVs (Cruzamentos) do MapBiomas. O **DuckDB Spatial** realiza a extração, limpeza de geometrias (`make_valid`) e conversão para Parquet em alta performance (8GB RAM limit), enquanto o **Google Earth Engine (GEE)** extrai métricas de radar (SRTM).
*   **2. Orquestração Atômica (Cosmos):** O **Astronomer Cosmos** converte o projeto dbt em tarefas isoladas no Airflow, permitindo *retries* parciais em caso de falhas em malhas geoespaciais pesadas.
*   **3. Transformação e Limpeza (Silver):** O **dbt** padroniza CRS, resolve duplicatas e unifica os schemas. Aqui aplicamos a regra do **Marco Temporal**, filtrando apenas alertas do MapBiomas posteriores a 22/07/2008.
*   **4. Inteligência Espacial (Gold):** *Push-down computation* no **Google BigQuery**. Joins espaciais massivos (`ST_INTERSECTS`) e lógica de adjacência geram as tabelas fato (`fct_compliance_risk`) contendo as flags de bloqueio e o status final de conformidade socioambiental.
*   **5. Consumo e Visualização:** 
    *   **Dashboard Streamlit:** Renderização via **PyDeck (WebGL)** para milhões de pontos.
    *   **API FastAPI:** Interface para integração de compliance em tempo real.

## ⚖️ Decisões de Arquitetura e Trade-offs

*   **MapBiomas como "Grande Juiz":** Em vez de recalcular todo o desmatamento do zero, utilizamos a inteligência dos Alertas Validados (Opção 3 e 4) como fonte da verdade, economizando processamento e aumentando a confiabilidade jurídica.
*   **DuckDB em vez de PostGIS (Camada Bronze):** O DuckDB Spatial permite o pré-processamento de Shapefiles complexos em memória e exportação para Parquet, eliminando a necessidade de manter um servidor PostgreSQL pesado para tarefas de ETL.
*   **dbt + BigQuery (Camada Silver/Gold):** Operações espaciais entre milhões de polígonos esgotariam a RAM rapidamente. Delegamos a matemática para os clusters distribuídos do BigQuery.
*   **Nix + uv:** O **Nix (via Devenv)** isola as versões de bibliotecas geoespaciais (GDAL, GEOS) de forma hermética, evitando o "DLL Hell".

## 🧪 Qualidade de Dados e Test-Driven Data Engineering

O motor do Caipora Sentinela conta com mais de **90 testes automatizados** via dbt tests, garantindo a integridade do veredito:

### 1. Testes de Regras de Negócio (Compliance)
*   `check_eligible_properties_mapbiomas_limit`: **(Crítico)** Garante matematicamente que nenhuma propriedade classificada como "ELEGÍVEL" possua sobreposição com alertas de desmatamento do MapBiomas após 2008.
*   `assert_embargo_post_2008_blocked`: Valida rigorosamente a regra do Marco Temporal para embargos do IBAMA.
*   `assert_no_eligible_on_slave_labor_land`: Garante o bloqueio definitivo por risco social (MTE).
*   `assert_adjacency_risk_flagged_correctly`: Valida o modelo de risco por contaminação de vizinhos.

### 2. Testes de Integridade Espacial
*   `assert_geometries_are_valid`: Valida se existem geometrias corrompidas ou não fechadas antes do processamento.
*   `accepted_range`: Garante que o NDVI esteja entre -1 e 1 e a inclinação entre 0 e 90°.

## 🧰 Stack Técnica

*   **Orquestração:** Apache Airflow 2.10 + Astronomer Cosmos
*   **Satélite & Auditoria:** **MapBiomas Alertas**, Google Earth Engine API (Sentinel-2 SR & NASA SRTM)
*   **Ingestão e Processamento:** DuckDB (Spatial), Python 3.11, PyArrow
*   **Transformação e Testes:** dbt Core (BigQuery Adapter)
*   **Data Lakehouse:** Google Cloud Storage & Google BigQuery
*   **Aplicações e APIs:** Streamlit, FastAPI, Uvicorn, Pydantic
*   **Visualização Espacial:** PyDeck (Deck.GL), Plotly, Folium
*   **Gerenciamento de Ambiente:** Devenv (Nix) + uv

## 🚦 Como Executar o Projeto

1.  **Inicialização do Ambiente:**
    Entre no shell isolado e suba os serviços de infraestrutura (Postgres, Airflow DB):
    ```bash
    devenv shell
    devenv up
    ```
    *(Mantenha este terminal rodando ou use `-d` se configurado)*

2.  **Airflow (Ingestão MapBiomas):**
    Em um novo terminal (dentro do `devenv shell`), inicie o scheduler/webserver e dispare a DAG:
    ```bash
    start-airflow  # Dispare a DAG ingestion_mapbiomas_intelligent
    ```

3.  **dbt (Transformação & Veredito):**
    Processe as camadas Silver/Gold e rode os testes de auditoria:
    ```bash
    dbt run --select +fct_compliance_risk
    dbt test
    ```

4.  **Dashboard:**
    Visualize os dados e o mapa de compliance:
    ```bash
    streamlit run app.py
    ```

## 🗺 Histórico e Evolução (Roadmap)

*   [x] **Fundação ELT:** Pipelines estruturados com DuckDB -> Parquet -> GCS -> BigQuery.
*   [x] **Governança com dbt:** Modelagem Silver/Gold e testes de integridade espacial.
*   [x] **Integração MapBiomas Alertas:** Ingestão híbrida (Shapefile + CSV), cruzamento com CAR e aplicação da regra de bloqueio do Código Florestal (O Grande Juiz).
*   [x] **Risco Social Avançado:** Algoritmo de **Fuzzy Matching** para geolocalizar infratores da "Lista Suja" do MTE no território.
*   [x] **Territórios e APPs:** Intersecção espacial massiva contra FUNAI, INCRA e ANA.
*   [x] **Ground Truth (Sensoriamento Remoto):** Detecção de anomalias de vegetação (NDVI Diferencial em APPs) e declividade via GEE.
*   [x] **API & Serving:** Estruturação base em FastAPI implementada.
*   [x] **Frontend & UX:** Dashboard interativo finalizado para upload de ativos e visualização de relatórios de conformidade.
*   [ ] **Integração Full-Stack:** Ajustes finais nos endpoints da API para otimizar a comunicação com o Frontend em tempo real.
*   [ ] **Escalabilidade Nacional:** Expansão da infraestrutura e dados para cobrir todo o território brasileiro.

---
## ⚖️ Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

**Autor:** Raphael Soares
*Framework de Geospatial Data Engineering aplicado à conformidade socioambiental e auditoria do mercado de carbono.*
# 🌿 Caipora Sentinela | Motor de Compliance Geoespacial para o Agronegócio

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://agromarte.agrimarketintel.com/)

> **Status:** ✅ Orquestração (Airflow + Cosmos) | 🧠 Motor de Regras (dbt + BigQuery) | 🛰️ Monitoramento Satelital (GEE) | 📊 Dashboard (Streamlit) | 🚀 API (FastAPI)

O **Caipora Sentinela** é um motor completo de **Geospatial Data & Analytics Engineering** focado na validação rigorosa de critérios ESG para a originação de crédito rural e mercado de carbono. Com foco estratégico no estado do **Mato Grosso (MT)**, a *engine* processa dados de mais de **211.000 propriedades**.

## 🚀 Evolução do Projeto: Da Experimentação à Produção

O projeto nasceu de uma necessidade de automatizar análises que antes eram manuais e fragmentadas. A arquitetura evoluiu significativamente:

*   **Fase 1 (Legado):** O fluxo dependia de **Jupyter Notebooks** e scripts de scraping instáveis para extrair dados do SIGEF e IBAMA. Usando **Python (pandas/geopandas)**, as transformações esbarravam em limites de memória local e os dados iam para o **DBeaver/PostGIS** para cruzamentos espaciais manuais.
*   **Fase 2 (Atual - O Nascimento do Caipora):** Para ganhar escala e governança corporativa, o sistema foi refatorado para uma arquitetura de **Data Lakehouse**, sendo batizado de **Caipora Sentinela**. Substituímos os notebooks por DAGs no **Airflow**, o PostGIS local pelo **BigQuery** (para processamento massivo distribuído) e o dbt passou a gerenciar a linhagem e os testes de conformidade.

## 🎯 O Problema de Negócio

Para garantir a integridade dos créditos de carbono, combater o *Greenwashing* e mitigar riscos na cadeia de suprimentos, o motor do Caipora Sentinela automatiza a resposta para:

1.  **Risco Social e Direitos Humanos:** Proprietários ou polígonos na "Lista Suja" de trabalho análogo à escravidão (MTE).
2.  **Proteção de Territórios Sensíveis:** Sobreposição com Terras Indígenas (FUNAI) ou Quilombolas (INCRA).
3.  **Embargos e Marco Temporal:** Invasão de áreas embargadas pelo IBAMA respeitando o Marco Temporal de julho de 2008.
4.  **Proteção Hídrica (APPs):** Respeito às Áreas de Preservação Permanente (cruzamento com ANA e IBGE).
5.  **Ground Truth (Sensoriamento Remoto):** A propriedade possui desmatamento real em APPs de topo de morro (declividade > 45°)? O índice de vegetação (NDVI) condiz com a preservação declarada?
6.  **Risco de Contaminação (Adjacency Risk):** A propriedade é vizinha de uma área desmatada ou embargada, sugerindo "lavagem" de commodities?

## 🏗 Arquitetura e Fluxo de Dados (Medallion Architecture)

O projeto processa dados de múltiplas fontes governamentais (SIGEF, CAR, IBAMA, MTE, ANA) e satelitais através de um fluxo estruturado:

*   **1. Ingestão & Grid Mapping (Bronze):** DAGs no Airflow extraem Shapefiles e organizam o território em Grids de 11km x 11km. O **DuckDB Spatial** realiza o pré-processamento local (conversão para Parquet), enquanto o **Google Earth Engine (GEE)** extrai métricas de radar (SRTM) e ópticas (Sentinel-2) em escala massiva.
*   **2. Orquestração Atômica (Cosmos):** O **Astronomer Cosmos** converte o projeto dbt em tarefas isoladas no Airflow, permitindo *retries* parciais em caso de falhas em malhas geoespaciais pesadas.
*   **3. Transformação e Limpeza (Silver):** O **dbt** padroniza CRS, resolve duplicatas e unifica os schemas. Tratamos pontos cegos climáticos (nuvens), classificando propriedades como `AWAITING SATELLITE CLEARANCE` para garantir a segurança do veredito.
*   **4. Inteligência Espacial (Gold):** *Push-down computation* no **Google BigQuery**. Joins espaciais massivos (`ST_INTERSECTS`) e lógica de adjacência geram as tabelas fato (`fct_compliance_risk`) contendo as flags de bloqueio e o status final de conformidade socioambiental.
*   **5. Consumo e Visualização:** 
    *   **Dashboard Streamlit:** Renderização via **PyDeck (WebGL)** para milhões de pontos.
    *   **API FastAPI:** Interface para integração de compliance em tempo real.

## ⚖️ Decisões de Arquitetura e Trade-offs

*   **GEE Push-down Raster Computation:** Em vez de baixar terabytes de imagens, enviamos a lógica para os servidores do Google. O GEE processa os pixels e devolve apenas o resultado tabular, economizando 99% de tráfego de rede e CPU local.
*   **DuckDB em vez de PostGIS (Camada Bronze):** O DuckDB Spatial permite o pré-processamento em memória e exportação para Parquet, eliminando a necessidade de manter um servidor PostgreSQL pesado para tarefas de ETL.
*   **dbt + BigQuery em vez de Pandas/GeoPandas (Camada Silver/Gold):** Operações espaciais entre milhões de polígonos esgotariam a RAM rapidamente. Delegamos a matemática para os clusters distribuídos do BigQuery.
*   **Nix + uv em vez de apenas Docker:** Bibliotecas geoespaciais (GDAL, GEOS) causam conflitos frequentes. O **Nix (via Devenv)** isola as versões de forma hermética, enquanto o **uv** garante um ambiente Python instantâneo e imutável.

## 🧪 Qualidade de Dados e Test-Driven Data Engineering

o motor do Caipora Sentinela conta com mais de 60 testes automatizados via **dbt tests**:

### 1. Testes de Integridade Espacial e Satélite
*   `accepted_range`: Garante que o NDVI esteja entre -1 e 1 e a inclinação entre 0 e 90°.
*   `assert_geometries_are_valid`: Valida se existem geometrias corrompidas ou não fechadas.
*   `check_eligible_properties_satellite_limit`: Impede que propriedades com alerta de desmatamento confirmado via satélite sejam classificadas como "Elegíveis".

### 2. Testes de Regras de Negócio (Compliance)
*   `assert_embargo_post_2008_blocked`: Valida rigorosamente a regra do Marco Temporal.
*   `assert_no_eligible_on_slave_labor_land`: Garante o bloqueio definitivo por risco social (MTE).
*   `assert_no_eligible_on_indigenous_land`: Bloqueio rígido em territórios protegidos.
*   `assert_adjacency_risk_flagged_correctly`: Valida o modelo de risco por contaminação de vizinhos.

## 🧰 Stack Técnica

*   **Orquestração:** Apache Airflow 2.10 + Astronomer Cosmos
*   **Satélite:** Google Earth Engine API (Sentinel-2 SR & NASA SRTM)
*   **Ingestão e Processamento:** DuckDB (Spatial), Python 3.11, PyArrow
*   **Transformação e Testes:** dbt Core (BigQuery Adapter)
*   **Data Lakehouse:** Google Cloud Storage & Google BigQuery
*   **Aplicações e APIs:** Streamlit, FastAPI, Uvicorn, Pydantic
*   **Visualização Espacial:** PyDeck (Deck.GL), Plotly, Folium
*   **Gerenciamento de Ambiente:** Devenv (Nix) + uv

## 🚦 Como Executar o Projeto

1.  **Ambiente:** 
    ```bash
    devenv shell
    ```
2.  **Airflow:** 
    ```bash
    start-airflow  # Dispare a DAG satellite_ground_truth_pipeline
    ```
3.  **dbt:** 
    ```bash
    dbt run --select +fct_compliance_risk
    ```
4.  **Dashboard:** 
    ```bash
    streamlit run app.py
    ```

## 🗺 Histórico e Evolução (Roadmap)

*   [x] **Fundação ELT:** Pipelines estruturados com DuckDB -> Parquet -> GCS -> BigQuery.
*   [x] **Governança com dbt:** Modelagem Silver/Gold e testes de integridade espacial.
*   [x] **Auditoria de Desmatamento:** Regras do Marco Temporal e embargos do IBAMA.
*   [x] **Risco Social:** Integração da "Lista Suja" do MTE (Trabalho Escravo).
*   [x] **Territórios e APPs:** Intersecção espacial contra FUNAI, INCRA e ANA.
*   [x] **Ground Truth (Sensoriamento Remoto):** Monitoramento de Relevo e NDVI via GEE para 211k propriedades no MT.
*   [x] **API & Serving:** Estruturação em FastAPI para consumo externo.
*   [ ] **Visualização Avançada:** Ajustando API e Dashboard para integração final de upload de ativos e geração de relatórios de conformidade via consultas em tempo real (Em desenvolvimento).

---
## ⚖️ Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

**Autor:** Raphael Soares
*Framework de Geospatial Data Engineering aplicado à conformidade socioambiental e auditoria do mercado de carbono.*
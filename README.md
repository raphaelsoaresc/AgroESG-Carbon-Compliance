# 🌿 AgroESG Carbon Compliance

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://agromarte.agrimarketintel.com/)

> **Status:** ✅ Orquestração (Cosmos) Ativa | 🧠 Motor de Compliance Operacional | 📊 Visualização (Front-end) Disponível

Este projeto é uma solução de **Analytics Engineering & Data Engineering** focada na validação de critérios ESG para originação de créditos de carbono e identificação de propriedades embargadas. A arquitetura evoluiu de um pipeline de extração simples para um ecossistema robusto que traduz o **Código Florestal Brasileiro** em regras de dados auditáveis e visualizáveis.

## 🎯 O Problema de Negócio

Para garantir a integridade de créditos de carbono e evitar o *Greenwashing*, é necessário auditar massivamente:
1.  **Sobreposição com Embargos:** A propriedade invade áreas embargadas pelo IBAMA?
2.  **Marco Temporal:** A infração ocorreu antes ou depois de julho de 2008 (Decreto Federal)?
3.  **Regras de Bioma:** A propriedade respeita a reserva legal específica (80% na Amazônia, 35% no Cerrado e 20% no Pantanal)?
4.  **Risco de Contaminação (Network Risk):** A propriedade é vizinha imediata de uma área desmatada, sugerindo "lavagem" de gado ou grãos?

## 🏗 Arquitetura e Fluxo de Dados

O projeto utiliza a **Medallion Architecture** (Bronze, Silver, Gold) para garantir a qualidade e rastreabilidade do dado:

*   **1. Camada Bronze (Ingestão):** Extração das fontes SIGEF (Fundiário) e IBAMA (Embargos) via **DuckDB Spatial**, convertendo Shapefiles brutos para **Parquet** otimizado e enviando ao Google Cloud Storage.
*   **2. Orquestração (Airflow + Cosmos):** O **Astronomer Cosmos** mapeia o projeto dbt e gera as tarefas no Airflow automaticamente (DAGs dinâmicas).
*   **3. Camada Silver (Transformação):** Limpeza, padronização de CRs (Sistemas de Coordenadas) e deduplicação lógica (*Last Record Wins*) via **dbt**.
*   **4. Camada Gold (Inteligência):** Processamento geoespacial no **BigQuery** para Joins espaciais massivos, aplicação do Marco Temporal e cálculo de risco.
*   **5. Camada de Apresentação (App):** Dashboard interativo em **Streamlit** com renderização geoespacial otimizada via **PyDeck**.

---

# 🚀 Diferenciais de Engenharia

### 1. Ingestão de Alta Performance (DuckDB + Parquet)
O pipeline utiliza o **DuckDB** com a extensão `spatial` para realizar o *pre-processing* local. Ele converte geometrias complexas em arquivos **Parquet** compactados antes do upload. Isso reduz drasticamente o custo de armazenamento e o tempo de I/O no Data Warehouse.

### 2. Visualização Otimizada (Mobile-First Strategy)
O front-end implementa estratégias avançadas de gerenciamento de memória:
*   **Renderização Híbrida:** Alterna automaticamente entre Polígonos (alta precisão) e Scatterplots (alta performance) dependendo do volume de dados e do dispositivo (Mobile/Desktop).
*   **Leitura Seletiva:** Otimização de tipos de dados (`float32`, `category`) para reduzir o consumo de RAM do navegador em até 80%.

### 3. Ambiente Hermético (Nix & uv)
O projeto utiliza **Nix** para gerenciar dependências a nível de sistema operacional (como as bibliotecas C++ do **GDAL/GEOS**). Combinado com o **uv**, isso garante um ambiente hermético, 100% reprodutível e imutável.

### 4. Estratégia ELT Geoespacial (Push-down Computation)
Em vez de processar geometrias pesadas em Python, o pipeline delega o processamento para o **BigQuery**. O dbt materializa as transformações dentro do Data Warehouse, permitindo escalar para milhões de polígonos (preparado para expansão Brasil).

### 5. Orquestração Atômica (Cosmos)
A integração via **Astronomer Cosmos** permite que cada modelo dbt seja uma tarefa individual no Airflow. Isso oferece observabilidade granular: se o cálculo de risco falhar, o Airflow permite reexecutar apenas aquela parte (**retries parciais**).

---

# 🧠 Lógica de Compliance (Geospatial Intelligence)

*   **Classificação de Biomas (IBGE):** Cruzamento espacial para determinar incidência na Amazônia Legal, Cerrado ou Pantanal.
*   **Veredito do Marco Temporal:** Bloqueio total para infrações pós-2008 na Amazônia e monitoramento para infrações anteriores.
*   **Risco por Contaminação (Adjacency Risk):** Identificação de polígonos que tocam áreas embargadas, prevenindo a "lavagem" de commodities irregulares.

---

# 🧰 Stack Técnica

*   **Ingestão:** DuckDB (Spatial Extension) + Python.
*   **Orquestração:** Apache Airflow 2.10 + Astronomer Cosmos.
*   **Transformação:** dbt Core (BigQuery Adapter).
*   **Data Lakehouse:** Google BigQuery & Cloud Storage.
*   **Visualização:** Streamlit + PyDeck (WebGL) + Plotly.
*   **Ambiente:** Gerenciado via `devenv` (Nix) e `uv`.

---

# 🚀 Como Executar o Projeto

### 1. Prepare o Ambiente
```bash
devenv shell
devenv up -d  # Inicia serviços locais (Postgres/Airflow)
```

### 2. Inicialize o Airflow
```bash
start-airflow
```

### 3. Execute o Dashboard (Front-end)
```bash
streamlit run app.py
```

---

# 🗺 Roadmap

* [x] **Infraestrutura:** Ambiente Nix com Postgres e Airflow configurados via `devenv`.
* [x] **Camada Bronze (Ingestão):** Pipelines DuckDB convertendo dados brutos para Parquet.
* [x] **Camada Silver (dbt):** Modelos de limpeza e deduplicação lógica.
* [x] **Camada Gold (dbt):** Implementação do Spatial Join e regras de Marco Temporal.
* [x] **Front-end:** Interface visual otimizada para Mobile com mapa de risco (Streamlit).
* [ ] **Sensoriamento Remoto (Ground Truth):** Integração com satélites (Sentinel-2/GEE) para validar uso do solo (NDVI) e alertas de desmatamento, mitigando a instabilidade de dados declaratórios (SIGEF).
* [ ] **Expansão Nacional:** Escalar a ingestão e processamento do BigQuery para cobrir todo o território brasileiro (Big Data Spatial).

---
**Autor:** Raphael Soares

*Projeto desenvolvido para portfólio de Data Engineering & Analytics.*

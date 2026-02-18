# 🌿 AgroESG Carbon Compliance

> **Status:** ✅ Orquestração (Cosmos) Ativa | 🧠 Motor de Compliance Operacional | 🚧 Visualização (Front-end) em Breve

Este projeto é uma solução de **Analytics Engineering & Data Engineering** focada na validação de critérios ESG para originação de créditos de carbono. A arquitetura evoluiu de um pipeline de extração simples para um ecossistema robusto que traduz o **Código Florestal Brasileiro** em regras de dados auditáveis.

## 🎯 O Problema de Negócio

Para garantir a integridade de créditos de carbono, é necessário auditar:
1.  **Sobreposição com Embargos:** A propriedade invade áreas embargadas pelo IBAMA?
2.  **Marco Temporal:** A infração ocorreu antes ou depois de julho de 2008?
3.  **Regras de Bioma:** A propriedade respeita a reserva legal específica do bioma (ex: 80% na Amazônia)?
4.  **Risco de Contaminação:** A propriedade é vizinha imediata de uma área desmatada?

## 🏗 Arquitetura e Fluxo de Dados

O projeto utiliza a **Medallion Architecture** (Bronze, Silver, Gold) para garantir a qualidade do dado:

*   **1. Camada Bronze (Ingestão):** Extração das fontes SIGEF e IBAMA via **DuckDB Spatial**, convertendo dados brutos para **Parquet** e armazenando no Google Cloud Storage.
*   **2. Orquestração (Airflow + Cosmos):** O **Astronomer Cosmos** mapeia o projeto dbt e gera as tarefas no Airflow automaticamente, garantindo a linhagem.
*   **3. Camada Silver (Transformação):** Limpeza, padronização e deduplicação lógica (*Last Record Wins*) via **dbt**.
*   **4. Camada Gold (Inteligência):** Processamento geoespacial no **BigQuery** para Joins espaciais, aplicação do Marco Temporal e cálculo de risco de contaminação.

---

# 🚀 Diferenciais de Engenharia

### 1. Ingestão de Alta Performance (DuckDB + Parquet)
O pipeline utiliza o **DuckDB** com a extensão `spatial` para realizar o *pre-processing* local. Ele converte Shapefiles e CSVs massivos em arquivos **Parquet** compactados. Isso reduz o volume de dados trafegados e acelera a carga no Data Warehouse.

### 2. Ambiente Hermético (Nix & uv)
O projeto utiliza **Nix** para gerenciar dependências a nível de sistema operacional (como as bibliotecas C++ do **GDAL/GEOS**). Combinado com o **uv**, isso garante um ambiente 100% reprodutível e imutável, eliminando o erro "funciona na minha máquina".

### 3. Estratégia ELT Geoespacial (Push-down Computation)
Em vez de processar geometrias pesadas em Python, o pipeline delega o processamento para o **BigQuery**. O dbt materializa as transformações dentro do Data Warehouse, permitindo escalar para milhões de polígonos aproveitando a computação distribuída.

### 4. Defensive Coding em SQL
Implementação de tratamentos robustos para geometrias inválidas via `SAFE.ST_GEOGFROMTEXT` e filtros de `SAFE_DIVIDE`. Isso impede que uma única geometria corrompida derrube o pipeline inteiro, garantindo resiliência operacional.

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
*   **Ambiente:** Gerenciado via `devenv` (Nix) e `uv`.

---

# 🚀 Como Executar o Projeto

### 1. Prepare o Ambiente
```bash
devenv shell
devenv up -d  # Inicia serviços locais
```

### 2. Inicialize o Airflow
```bash
start-airflow
```

### 3. Documentação e Linhagem
```bash
dbt docs generate && dbt docs serve
```

---

# 🗺 Roadmap Atualizado

* [x] **Infraestrutura:** Ambiente Nix com Postgres e Airflow configurados via `devenv`.
* [x] **Camada Bronze (Ingestão):** Pipelines DuckDB convertendo dados brutos para Parquet e enviando ao GCS.
* [x] **Camada Silver (dbt):** Modelos de limpeza e deduplicação lógica.
* [x] **Camada Gold (dbt):** Implementação do Spatial Join e regras de Marco Temporal.
* [ ] **Front-end:** Interface visual para exibir o mapa de risco (Streamlit).
* [ ] **API:** Expor os resultados de compliance via REST API.

---
**Autor:** Raphael Soares

*Projeto desenvolvido para portfólio de Data Engineering & Analytics.*
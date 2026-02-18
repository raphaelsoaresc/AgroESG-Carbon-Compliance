# 🌿 AgroESG Carbon Compliance

> **Status:** ✅ Orquestração (Cosmos) Ativa | 🧠 Motor de Compliance Operacional | 🚧 Visualização (Front-end) em Breve

Este projeto é uma solução de **Analytics Engineering & Data Engineering** focada na validação de critérios ESG para originação de créditos de carbono. A arquitetura evoluiu de um pipeline de extração simples para um ecossistema robusto que traduz o **Código Florestal Brasileiro** em regras de dados auditáveis.

## 🏗 Arquitetura e Stack

O projeto utiliza uma abordagem **Medallion Architecture** (Bronze, Silver, Gold) orquestrada por um ambiente imutável.

    graph TD
        subgraph "Camada Bronze (Ingestão)"
            A[Fontes: SIGEF & IBAMA] -->|DuckDB Spatial| B[Parquet Files / GCS]
        end

        subgraph "Orquestração Dinâmica (Astronomer Cosmos)"
            B --> C{Airflow DAG}
            C -->|Renderiza| D[dbt Core Models]
        end

        subgraph "Transformação & Inteligência (Silver/Gold)"
            D --> E[Limpeza & Padronização]
            E --> F[Geospatial Joins (BigQuery Geo)]
            F --> G[Regras de Negócio: Marco Temporal & Biomas]
            G --> H[Cálculo de Risco por Contaminação]
        end

        H --> I[Tabela Final: Compliance Risk]

---

# 🚀 Diferenciais de Engenharia

### 1. Ingestão de Alta Performance (DuckDB + Parquet)
Em vez de carregar dados brutos diretamente no Data Warehouse, o pipeline utiliza o **DuckDB** com a extensão `spatial` para realizar o *pre-processing* local. Ele converte Shapefiles e CSVs massivos em arquivos **Parquet** altamente compactados e tipados. Isso reduz o volume de dados trafegados para o Cloud Storage e acelera drasticamente a carga no BigQuery.

### 2. Ambiente Hermético (Nix & uv)
Ao contrário do padrão venv, o projeto utiliza **Nix** para gerenciar dependências a nível de sistema operacional (como as bibliotecas C++ do **GDAL/GEOS** necessárias para geoespacial). Combinado com o **uv**, isso garante um ambiente 100% reprodutível e imutável, eliminando o clássico "funciona na minha máquina".

### 3. Estratégia ELT Geoespacial (Push-down Computation)
Em vez de processar geometrias pesadas em Python (Pandas/Geopandas), o pipeline delega o processamento para o **BigQuery**. O dbt materializa as transformações dentro do Data Warehouse, permitindo escalar de milhares para milhões de polígonos sem estourar memória RAM, aproveitando a computação distribuída da nuvem.

### 4. Defensive Coding em SQL
Implementação de tratamentos robustos para geometrias inválidas. O uso de funções como `SAFE.ST_GEOGFROMTEXT` e filtros de `SAFE_DIVIDE` nos modelos stg e int impede que uma única geometria corrompida no SIGEF/IBAMA derrube o pipeline inteiro, garantindo resiliência operacional.

### 5. Orquestração Atômica (Cosmos)
A integração via **Astronomer Cosmos** permite que cada modelo dbt (stg, int, marts) seja tratado como uma tarefa individual no Airflow. Isso oferece observabilidade granular: se o cálculo de risco falhar, o Airflow aponta exatamente qual modelo quebrou e permite reexecutar apenas aquela parte (**retries parciais**), sem reprocessar a ingestão bruta.

---

# 🧠 Lógica de Compliance (Geospatial Intelligence)

O coração do projeto reside nas regras de negócio codificadas em SQL via **dbt**:

*   **Classificação de Biomas (IBGE):** Cruzamento espacial para determinar se a propriedade incide na Amazônia Legal, Cerrado ou Mata Atlântica.
*   **Veredito do Marco Temporal:**
    *   *Infrações pós-2008 na Amazônia:* Risco Crítico (Bloqueio Total).
    *   *Infrações pré-2008:* Elegível sob Monitoramento (conforme legislação vigente).
*   **Risco por Contaminação (Adjacency Risk):**
    *   O sistema identifica polígonos elegíveis que tocam áreas embargadas.
    *   Isso previne o "vazamento" de commodities de áreas irregulares para áreas certificadas.

---

# 🧰 Stack Técnica

*   **Ingestão:** DuckDB (Spatial Extension) + Python.
*   **Orquestração:** Apache Airflow 2.10 + Astronomer Cosmos.
*   **Transformação:** dbt Core (BigQuery Adapter).
*   **Data Lakehouse:** Google BigQuery & Cloud Storage (Parquet format).
*   **Ambiente:** Gerenciado via `devenv` (Nix) e `uv`.

---

# 🚀 Como Executar o Projeto

O ambiente é gerenciado via **Nix**, dispensando instalações manuais complexas.

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
Para visualizar a linhagem dos dados e as regras aplicadas:
```bash
dbt docs generate && dbt docs serve
```

---

# 🗺 Roadmap Atualizado

* [x] **Infraestrutura:** Ambiente Nix com Postgres e Airflow configurados via `devenv`.
* [x] **Camada Bronze (Ingestão):** Pipelines DuckDB convertendo dados brutos (SIGEF/IBAMA) para Parquet e enviando ao GCS.
* [x] **Camada Silver (dbt):** Modelos de limpeza e deduplicação lógica (*Last Record Wins*).
* [x] **Camada Gold (dbt):** Implementação do Spatial Join e regras de Marco Temporal.
* [ ] **Front-end:** Desenvolvimento de interface visual para exibir o mapa de risco (Streamlit).
* [ ] **API:** Expor os resultados de compliance via REST API.

---
**Autor:** Raphael Soares

*Projeto desenvolvido para portfólio de Data Engineering & Analytics.*
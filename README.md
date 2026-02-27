# 🌿 AgroESG Carbon Compliance

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://agromarte.agrimarketintel.com/)

> **Status:** ✅ Orquestração (Airflow + Cosmos) | 🧠 Motor de Regras (dbt + BigQuery) | 📊 Dashboard Geoespacial (Streamlit) | 🚀 API (FastAPI)

Este projeto é uma plataforma completa de **Geospatial Data & Analytics Engineering** focada na validação rigorosa de critérios ESG para a originação de créditos de carbono e auditoria de risco socioambiental em propriedades rurais no Brasil.

A arquitetura evoluiu de um pipeline de extração simples para um ecossistema complexo de inteligência de dados que traduz o **Código Florestal Brasileiro** e regulamentações trabalhistas em regras de *compliance* auditáveis, massivas e automatizadas.

## 🎯 O Problema de Negócio

Para garantir a integridade dos créditos de carbono, combater o *Greenwashing* e mitigar riscos na cadeia de suprimentos (originação de grãos/gado), o sistema automatiza a resposta para:

1. **Risco Social e Direitos Humanos:** Os proprietários ou polígonos constam na "Lista Suja" de Trabalho Escravo (MTE)?
2. **Proteção de Territórios Sensíveis:** Há sobreposição com Terras Indígenas (FUNAI) ou Quilombolas (INCRA)?
3. **Embargos e Desmatamento:** A propriedade invade áreas embargadas pelo IBAMA? A infração respeita o Marco Temporal de julho de 2008?
4. **Proteção Hídrica (APPs):** A propriedade respeita as Áreas de Preservação Permanente ao redor de corpos d'água (cruzamento com dados da ANA e IBGE)?
5. **Risco de Contaminação (Adjacency Risk):** A propriedade é vizinha de uma área desmatada ou embargada, sugerindo "lavagem" de commodities?

## 🏗 Arquitetura e Fluxo de Dados (Medallion Architecture)

O projeto processa dados de múltiplas fontes governamentais (SIGEF, CAR, IBAMA, MTE, ANA, FUNAI, IBGE) através de um fluxo estruturado:

* **1. Ingestão (Bronze):** DAGs especializadas no Airflow extraem *Shapefiles* e listas públicas. O **DuckDB Spatial** é utilizado como motor de pré-processamento local, convertendo geometrias complexas em **Parquet** otimizado antes do envio ao Google Cloud Storage (GCS).
* **2. Orquestração Atômica (Cosmos):** O **Astronomer Cosmos** lê o projeto dbt e o converte dinamicamente em tarefas isoladas no Airflow, permitindo *retries* parciais em caso de falhas em malhas geoespaciais pesadas.
* **3. Transformação e Limpeza (Silver):** O **dbt** padroniza CRS (Sistemas de Coordenadas Geográficas), resolve duplicatas (*Last Record Wins*) e unifica os schemas no Data Warehouse (`agro_esg_staging`).
* **4. Inteligência Espacial (Gold):** *Push-down computation* no **Google BigQuery**. Joins espaciais massivos (como `ST_INTERSECTS`) são executados nativamente para gerar as tabelas fato (`fct_compliance_risk`) nos schemas `agro_esg_intermediate` e `agro_esg_marts`.
* **5. Consumo e Visualização (em desenvolvimento):**
    * **Dashboard Streamlit:** Renderização de milhões de pontos e polígonos usando **PyDeck (WebGL)**, dividida em clusters de auditoria (Total, Bloqueios Críticos, Originação, Risco).
    * **API (FastAPI + Pydantic):** Interface programática para integração da validação de *compliance* em tempo real com sistemas externos.

## ⚖️ Decisões de Arquitetura e *Trade-offs*

Em um projeto com manipulação massiva de dados geoespaciais, a escolha das ferramentas dita a viabilidade e o custo do pipeline. Veja as razões das nossas escolhas:

* **DuckDB em vez de PostGIS (Camada Bronze):**
    * *Por quê?* O **DuckDB (Spatial)** permite fazer o pré-processamento analítico *em memória* e exportar diretamente para arquivos Parquet compactados, sem a necessidade de manter um servidor PostgreSQL pesado rodando constantemente.
    * *O Ganho:* Redução drástica no custo de armazenamento no Data Lake e diminuição no tempo de I/O de rede.

* **dbt + BigQuery em vez de Pandas/GeoPandas (Camada Silver/Gold):**
    * *Por quê?* Operações como `ST_INTERSECTS` para validar cruzamentos entre milhões de polígonos esgotam a memória RAM (OOM) rapidamente em *workers* Python tradicionais.
    * *O Ganho:* Com o *Push-down computation*, delegamos a matemática complexa das geometrias para os clusters distribuídos do BigQuery via dbt, garantindo escalabilidade infinita.

* **Astronomer Cosmos em vez de Airflow DAGs tradicionais:**
    * *Por quê?* Rodar o dbt no Airflow normalmente envolve um único bloco (`dbt run`). Se o modelo final falhar, perde-se a rastreabilidade.
    * *O Ganho:* O Cosmos converte cada modelo dbt em uma *task* atômica do Airflow, oferecendo observabilidade granular e permitindo *retries* parciais.

* **Nix + `uv` em vez de apenas Docker (Gestão de Ambiente):**
    * *Por quê?* Bibliotecas geoespaciais em Python dependem de bibliotecas de baixo nível em C++ (*GDAL, GEOS*) que frequentemente causam conflitos de dependência ("dependency hell").
    * *O Ganho:* O ecossistema **Nix** isola as versões ao nível do sistema operacional de forma hermética. Combinado com o `uv`, o ambiente é instantâneo, imutável e sem o *overhead* do Docker no desenvolvimento local.

## 🧪 Qualidade de Dados e *Test-Driven Data Engineering*

Devido à natureza crítica e auditável do projeto, o sistema conta com uma extensa bateria de testes singulares e genéricos implementados via **dbt tests**:

### 1. Testes de Integridade Espacial
* `assert_geometries_are_valid`: Valida se existem geometrias corrompidas, nulas ou não fechadas.
* `assert_no_negative_areas`: Impede a existência de polígonos com área negativa após conversões de CRS.
* `assert_overlap_not_greater_than_property`: Garante que as áreas de sobreposição (ex: embargos) nunca sejam matematicamente maiores do que a área da propriedade original.

### 2. Testes de Regras de Negócio (*Compliance Strictness*)
* `assert_embargo_post_2008_blocked`: Valida a regra do **Marco Temporal**, garantindo que nenhuma propriedade com embargo pós-julho de 2008 seja classificada como "Elegível".
* `assert_no_eligible_on_indigenous_land` / `assert_no_eligible_on_quilombola_land`: Bloqueio rígido (*hard block*) caso a propriedade sobreponha territórios de povos originários.
* `assert_no_eligible_on_slave_labor_land`: Garante que o cruzamento com a "Lista Suja" do MTE resulte em um bloqueio definitivo.
* `assert_adjacency_risk_flagged_correctly`: Valida o modelo de *network risk*, atestando que fazendas vizinhas a polígonos embargados recebam a *flag* de risco de contaminação.

### 3. Testes de Consistência Lógica
* `assert_car_owners_uniqueness`: Garante que a lógica de deduplicação funcionou.
* `assert_fuzzy_match_did_not_explode`: Protege as junções de identidade contra a geração de registros duplicados (*fan-out* acidental).

## 🚀 Diferenciais de Engenharia

* **Sensoriamento Remoto Pronto para Uso:** A stack já integra o `earthengine-api` e `geemap`, preparando o terreno para análises de *Ground Truth* baseadas em satélites (Sentinel/Landsat), validando índices como NDVI contra os dados declaratórios do CAR.
* **Visualização Mobile-First e Híbrida:** O Streamlit utiliza renderização adaptativa no PyDeck, alternando entre polígonos e *scatterplots* dependendo do volume de dados para poupar memória no navegador do cliente.

## 🧰 Stack Técnica

* **Orquestração:** Apache Airflow 2.10 + Astronomer Cosmos
* **Ingestão e Processamento:** DuckDB (Spatial), Python 3.11, PyArrow
* **Transformação e Testes:** dbt Core (BigQuery Adapter)
* **Data Lakehouse:** Google Cloud Storage & Google BigQuery
* **Aplicações e APIs:** Streamlit, FastAPI, Uvicorn, Pydantic
* **Visualização Espacial:** PyDeck (Deck.GL), Plotly, Folium
* **Gerenciamento de Ambiente:** Devenv (Nix) + uv

## 🚦 Como Executar o Projeto

### 1. Preparar o Ambiente (Nix/uv)
```bash
devenv shell
devenv up -d  # Inicia os serviços locais em background (Postgres)
```

### 2. Inicializar a Orquestração (Airflow)
O `devenv` já executa as migrações e cria o usuário admin automaticamente.
```bash
start-airflow
```
Acesse `http://localhost:8080` para visualizar as DAGs de ingestão e transformação (dbt via Cosmos).

### 3. Executar o Dashboard de Auditoria (Front-end)
```bash
streamlit run app.py
```

## 🗺 Histórico e Evolução (Roadmap)

* [x] **Fundação ELT:** Pipelines estruturados com DuckDB -> Parquet -> GCS -> BigQuery.
* [x] **Governança com dbt:** Modelagem Silver/Gold e testes de integridade espacial implementados.
* [x] **Auditoria de Desmatamento:** Regras do Marco Temporal e embargos do IBAMA aplicadas em escala.
* [x] **Risco Social:** Integração da "Lista Suja" do MTE (Trabalho Escravo) cruzada via CPF/CNPJ.
* [x] **Territórios e APPs:** Intersecção espacial automatizada contra FUNAI, INCRA e bases hídricas da ANA.
* [x] **API & Serving:** Estruturação em FastAPI para consumo externo.
* [ ] **Ground Truth (Sensoriamento Remoto):** Validar limites das propriedades declaradas via Google Earth Engine (detecção de uso de solo em tempo real).

---
## ⚖️ Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---
**Autor:** Raphael Soares

*Framework de Data & Analytics Engineering aplicado à conformidade socioambiental e auditoria do mercado de carbono.*
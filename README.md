# 🌿 AgroESG Carbon Compliance

> **Status:** 🚀 Ingestão (EL) Operacional | 🏗️ Transformação (dbt) em Desenvolvimento

Este projeto é uma solução de **Engenharia de Dados (ELT)** focada em análise de risco e compliance ambiental para a originação de créditos de carbono. O objetivo é cruzar dados geoespaciais de propriedades rurais (SIGEF) com listas de embargos ambientais (IBAMA), garantindo a elegibilidade ESG através de uma arquitetura resiliente e idempotente.

## 🎯 O Problema de Negócio

Para emitir créditos de carbono de alta integridade, é necessário garantir que a área do projeto não possui sobreposição com áreas embargadas. O desafio reside na instabilidade das fontes governamentais e na complexidade dos dados geoespaciais. Este projeto implementa um pipeline que garante a **rastreabilidade histórica** e a **unicidade dos dados**, mesmo em casos de reprocessamento.

## 🏗 Arquitetura e Stack

O projeto utiliza uma abordagem **Medallion Architecture** (Bronze, Silver, Gold) orquestrada por um ambiente imutável via Nix.

graph TD
    subgraph "Ingestão & Pré-Processamento (Local/DuckDB)"
        A[Arquivos Brutos: SIGEF/IBAMA] --> B[DuckDB Spatial]
        B -->|Hash MD5 & Parquet| C[Local Staging]
    end
    
    subgraph "Cloud Storage (Bronze Layer)"
        C -->|Upload Idempotente| D[Google Cloud Storage]
        D -->|Write Append| E[BigQuery Raw Tables]
    end

    subgraph "Transformação (Cloud/dbt)"
        E --> F[dbt Core: Silver Layer]
        F -->|Deduplicação & Spatial Join| G[BigQuery Gold: Compliance]
    end


### 🛠️ Destaques de Engenharia de Dados

*   **Idempotência Garantida:** Implementação de Hashing MD5 para cada arquivo processado. O pipeline utiliza nomes determinísticos no GCS para evitar lixo no storage e metadados de auditoria (`file_hash`, `ingested_at`) no BigQuery.
*   **Processamento Espacial de Alta Performance:** Uso do **DuckDB** para leitura de Shapefiles complexos e conversão local para Parquet. A geometria é tratada via `ST_AsText` para garantir compatibilidade total com o BigQuery.
*   **Resiliência no Airflow 2.10:** Superação de bugs de serialização de metadados (JSON/Pickle) através da implementação do `BigQueryInsertJobOperator`, garantindo uma comunicação robusta com a API de Jobs do Google Cloud.
*   **Estratégia de Housekeeping:** Sistema automático de arquivamento de arquivos processados, prevenindo reprocessamentos infinitos e garantindo a limpeza do ambiente local.

## 🧰 Stack Técnica

*   **Orquestração:** Apache Airflow 2.10 (rodando com Postgres Backend e LocalExecutor).
*   **Motor de Dados:** DuckDB (com extensões Spatial e HTTPFS).
*   **Data Warehouse:** Google BigQuery & Cloud Storage.
*   **Transformação:** dbt Core (em implementação).
*   **Infraestrutura:** `devenv` (Nix) e `uv` para ambientes 100% reprodutíveis.

## 🚀 Como Executar o Projeto

Este projeto utiliza **Nix**. Não é necessário instalar Python, Postgres ou dependências manualmente.

### Passo a Passo

1.  **Entre no shell de desenvolvimento:**
    ```bash
    devenv shell
    ```
    *Isso prepara o ambiente isolado com Python, `uv`, Postgres e as dependências do Airflow.*

2.  **Ative os serviços de infraestrutura:**
    ```bash
    devenv up -d
    ```
    *Inicia o banco Postgres em segundo plano (essencial para o Metastore do Airflow).*

3.  **Inicie o Orquestrador:**
    ```bash
    start-airflow
    ```
    *Acesse `localhost:8080`. Credenciais padrão: `admin` / `admin`.*

4.  **Configuração de Conexões (Airflow UI):**
    *   **`fs_default`**: Tipo `File (path)`, aponte o `Extra` ou `Path` para a raiz do seu diretório de dados local.
    *   **`google_cloud_default`**: Tipo `Google Cloud`, insira o JSON da sua Service Account para permissões no BigQuery e GCS.


## 🗺 Roadmap

* [x] **Infraestrutura:** Ambiente Nix com Postgres e Airflow configurados.
* [x] **Ingestão SIGEF:** DAG idempotente com DuckDB Spatial e carga no BigQuery.
* [x] **Ingestão IBAMA:** DAG resiliente com suporte a CSV/Shapefile e carga via BigQuery Jobs.
* [ ] **Camada Silver (dbt):** Modelos de limpeza e deduplicação lógica (Last Record Wins).
* [ ] **Camada Gold (dbt):** Implementação do Spatial Join para detecção de sobreposições.
* [ ] **Dashboard:** Visualização de risco ESG no Looker Studio.

---
**Autor:** Raphael Soares

*Projeto desenvolvido para portfólio de Data Engineering & Analytics.*
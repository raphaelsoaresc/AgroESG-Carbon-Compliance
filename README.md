# 🌿 AgroESG Carbon Compliance

> **Status:** 🚀 Infraestrutura Operacional | 🚧 Refatoração (Migração de Scripts `src/` para Airflow)

Este projeto é uma solução de **Engenharia de Dados (ELT)** focada em análise de risco e compliance ambiental para a originação de créditos de carbono. O objetivo é cruzar dados geoespaciais de propriedades rurais com listas de embargos ambientais (IBAMA) e malhas fundiárias (SIGEF), garantindo a elegibilidade ESG através de uma arquitetura resiliente.

## 🎯 O Problema de Negócio

Para emitir créditos de carbono de alta integridade, é necessário garantir que a área do projeto não possui sobreposição com áreas embargadas. Porém, fontes governamentais são instáveis, mudam formatos sem aviso e bloqueiam requisições automatizadas. Este projeto cria um **"Bunker de Dados"** para garantir a ingestão contínua, mesmo em cenários hostis.

## 🏗 Arquitetura e Stack

O projeto segue uma abordagem **Híbrida (Local Stealth + Cloud Performance)**, utilizando Nix para infraestrutura imutável.

* **Ingestão Resiliente (Airflow + Tor):** Extração anônima via rede Tor para evitar bloqueios de IP e *fingerprinting* TLS (`curl_cffi`).
* **Pré-processamento (DuckDB):** Conversão local de CSVs gigantes para Parquet com tipagem forte e verificação de Hash (Idempotência).
* **Data Warehouse (Google BigQuery):** Armazenamento escalável dos dados brutos e tratados.
* **Transformação (dbt Core):** Modelagem de dados e regras de negócio executadas diretamente no BigQuery.
* **Gerenciamento de Ambiente:** `devenv` (Nix) para orquestrar serviços (Tor, Postgres, Airflow) sem sujar o sistema operacional.

## ⚙️ Funcionalidades Implementadas (Scripts `src/`)

A lógica atual reside em scripts Python robustos (`src/`) que estão sendo migrados para DAGs do Airflow:

1.  **Extração "Anti-Bloqueio" (IBAMA):**
    *   Simulação de navegador real (Chrome) para bypass de firewall.
    *   **Fallback Automático:** Se o site oficial cair, o sistema busca o backup mais recente no Google Cloud Storage para não quebrar o dashboard.
    *   **Zero Desperdício:** Validação de ETag/Hash MD5 antes do processamento. Se o dado não mudou, o pipeline para.

2.  **Tratamento Geoespacial (SIGEF):**
    *   Leitura de Shapefiles complexos e conversão para WKT (Well-Known Text).
    *   Padronização de tipagem para garantir integridade na camada Raw do BigQuery.

## 🚀 Como Executar o Projeto

Este projeto utiliza **Nix** e **Devenv**. Não é necessário instalar Python, GDAL ou Banco de Dados manualmente.

### Pré-requisitos
* Instalar [Nix](https://nixos.org/download.html)
* Instalar [Devenv](https://devenv.sh/getting-started/)

### Passo a Passo

1.  **Inicie a Infraestrutura (Tor + Postgres):**
    ```bash
    devenv up
    ```
    *Isso iniciará o Proxy Tor (porta 9050) e o PostgreSQL em segundo plano.*

2.  **Entre no shell de desenvolvimento:**
    ```bash
    devenv shell
    ```
    *Na primeira execução, o Airflow será instalado e configurado automaticamente.*

3.  **Inicie o Orquestrador:**
    ```bash
    start-airflow
    ```
    *Acesse `localhost:8080` com a senha gerada no terminal.*

4.  **Valide a Conexão Híbrida:**
    ```bash
    check-connection
    ```
    *Deve retornar um IP do Tor (Ingestão) e seu IP Real (Upload).*

## 🗺 Roadmap (Refatoração & Analytics)

O foco atual é portar a inteligência dos scripts Python isolados para a estrutura gerenciável do Airflow:

* [x] **Infraestrutura:** Ambiente Nix com Tor, Airflow e DuckDB configurados.
* [ ] **Refatoração (Ingestão):** Converter `src/extract_load_ibama.py` para DAG do Airflow.
* [ ] **Refatoração (Geo):** Converter `src/load_sigef_raw.py` para DAG do Airflow.
* [ ] **Data Warehouse:** Configurar tabelas Raw no BigQuery.
* [ ] **Transformação (dbt):** Criar modelos `stg` (limpeza) e `marts` (regras de compliance).

---
**Autor:** Raphael Soares
*Projeto desenvolvido para portfólio de Data Engineering & Analytics.*
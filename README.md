# 🌿 AgroESG Carbon Compliance

> **Status:** 🚧 Em Desenvolvimento (Migração para dbt em andamento)

Este projeto é uma solução de **Engenharia de Analytics** focada em análise de risco e compliance ambiental para a originação de créditos de carbono. O objetivo é cruzar dados geoespaciais de propriedades rurais com listas de embargos ambientais (IBAMA) no estado de Mato Grosso, garantindo a elegibilidade ESG.

## 🎯 O Problema de Negócio

Para emitir créditos de carbono de alta integridade, é necessário garantir que a área do projeto não possui sobreposição com áreas embargadas por desmatamento ou outras infrações ambientais. Este projeto automatiza a ingestão, limpeza e transformação desses dados para permitir auditorias rápidas.

## 🏗 Arquitetura e Stack

O projeto segue uma abordagem **Modern Data Stack**, utilizando contêineres e gerenciamento declarativo de ambiente.

* **Ingestão & Transformação (Python/GeoPandas):** Extração de dados brutos do IBAMA (CSV), limpeza de geometrias (WKT) e carga no banco de dados.
* **Data Warehouse (PostgreSQL + PostGIS):** Armazenamento de dados espaciais.
* **Transformação (dbt Core):** Modelagem de dados, testes de qualidade e documentação (em implementação).
* **Gerenciamento de Ambiente:** `devenv` (Nix) para isolamento do sistema e `uv` para dependências Python ultra-rápidas.

## ⚙️ Funcionalidades Implementadas (ETL)

O pipeline atual de ingestão (`notebooks/01_etl...`) já realiza:

1.  **Tratamento Geoespacial:**
    * Conversão de coordenadas (Latitude/Longitude) e geometrias WKT.
    * Criação de *buffer* de 50 metros em pontos para transformar em polígonos.
    * Reprojeção para SIRGAS 2000 / UTM 22S (EPSG:31982) para cálculos métricos precisos.
    * Correção de geometrias inválidas (ex: *bowtie polygons*) antes da carga no PostGIS.

2.  **Regras de Compliance (Categorização):**
    Classificação automática dos embargos com base na data da infração e código florestal:
    * `active_enforcement`: Embargos recentes e ativos.
    * `consolidated_area`: Áreas consolidadas (anteriores a 2008).
    * `recent_violation`: Infrações nos últimos 5 anos.
    * `insufficient_data`: Falta de dados históricos.

## 🚀 Como Executar o Projeto

Este projeto utiliza **Nix** e **Devenv** para garantir que você tenha todas as dependências (GDAL, GEOS, Python, PostGIS) sem sujar seu sistema operacional.

### Pré-requisitos
* Instalar [Nix](https://nixos.org/download.html)
* Instalar [Devenv](https://devenv.sh/getting-started/)

### Passo a Passo

1.  **Inicie o ambiente e serviços (Banco de Dados):**
    ```bash
    devenv up
    ```
    *Isso iniciará o PostgreSQL com as extensões PostGIS automaticamente.*

2.  **Entre no shell de desenvolvimento:**
    ```bash
    devenv shell
    ```

3.  **Instale/Sincronize as dependências Python:**
    ```bash
    uv sync
    ```

4.  **Execute o pipeline de carga (Exemplo):**
    ```bash
    # Executa o notebook de ingestão via linha de comando
    jupyter execute notebooks/01_etl_ibama_embargos_mt_postgis.ipynb
    ```

5.  **Rodar modelos dbt (Em construção):**
    ```bash
    cd agro_credit_transform
    dbt debug
    dbt run
    ```

## 🗺 Roadmap (Migração para dbt)

Como próximo passo na jornada de Analytics Engineering, a lógica complexa que hoje reside nos Notebooks Python será migrada para modelos SQL no dbt:

* [ ] **Staging:** Criar `stg_ibama_embargos` (View materializada do Raw Data).
* [ ] **Intermediate:** Mover a lógica de categorização (`compliance_status`) para SQL.
* [ ] **Marts:** Criar tabela fato de análises de risco por município.
* [ ] **Tests:** Implementar testes (ex: garantir que não existem geometrias nulas na tabela final).

---
**Autor:** Raphael Soares
*Projeto desenvolvido para portfólio de Analytics Engineering.*
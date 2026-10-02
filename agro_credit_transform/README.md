# Agro Credit Transform

Projeto dbt para transformar e validar dados geoespaciais e administrativos do setor agropecuário brasileiro, com foco em compliance ambiental, risco territorial e elegibilidade de propriedades rurais.

Este pacote foi desenvolvido para alimentar a lógica de decisão do sistema AgroESG Carbon Compliance, combinando dados do CAR, SIGEF, IBAMA, MapBiomas, MTE, INCRA, ICMBio, ANA, IBGE e outras bases georreferenciadas em um pipeline analítico em BigQuery.

## Visão geral

O projeto organiza os dados em três camadas principais:

- Staging: padronização e ingestão das fontes brutas
- Intermediate: cruzamentos espaciais, normalização, regras de negócio e métricas forenses
- Marts: tabelas analíticas finais de risco, elegibilidade e inteligência de investimento

O objetivo central é avaliar se uma propriedade rural pode ser considerada elegível, bloqueada ou sujeita a revisão manual, considerando:

- desmatamento e alertas de satélite
- áreas de preservação permanente (APP)
- embargos ambientais
- reserva legal
- sobreposição com terras indígenas, quilombos, assentamentos e UC
- trabalho análogo à escravidão
- risco geoespacial e de adjacência
- inconsistência de área e dados do CAR

## Estrutura do projeto

```text
agro_credit_transform/
├── README.md
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── package-lock.yml
├── macros/
│   ├── generate_schema_name.sql
│   ├── get_compliance_logic.sql
│   ├── get_neighbor_barriers_logic.sql
│   ├── sanitize_encoding.sql
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_ibama.sql
│   │   ├── stg_sigef.sql
│   │   ├── stg_ibge_biomes.sql
│   │   ├── stg_mte_slave_labor.sql
│   │   ├── stg_ibge__rodovias_risco.sql
│   │   ├── stg_ana__rios_app.sql
│   │   ├── stg_property_topography.sql
│   │   ├── stg_incra_quilombola_lands.sql
│   │   ├── stg_funai_indigenous_lands.sql
│   │   ├── stg_icmbio__unidades_conservacao.sql
│   │   ├── ...
│   ├── intermediate/
│   │   ├── int_compliance__property_analysis.sql
│   │   ├── int_compliance__adjacency_scoring.sql
│   │   ├── int_compliance__infrastructure_bridge.sql
│   │   ├── int_property_embargo_overlap.sql
│   │   ├── int_mapbiomas_deforestation.sql
│   │   ├── int_satellite_metrics_persistent.sql
│   │   ├── int_brazil_reference_geometries.sql
│   │   ├── int_all_embargoes.sql
│   │   ├── int_compliance_forensic_shapes.sql
│   │   ├── ...
│   ├── marts/
│   │   ├── fct_compliance_risk.sql
│   │   ├── fct_property_investment_intelligence.sql
│   │   ├── fct_territorial_invasion_forensics.sql
│   │   ├── fct_social_impact_slave_labor.sql
│   │   ├── fct_compliance_geometries_mart.sql
├── seeds/
│   ├── legal_parameters.csv
│   ├── br_ibge_cities.csv
│   ├── br_state_codes.csv
├── snapshots/
│   ├── compliance_history.sql
│   ├── snap_all_embargoes.sql
├── tests/
│   ├── assert_embargo_post_2008_blocked.sql
│   ├── assert_no_eligible_on_social_risk_areas.sql
│   ├── assert_no_eligible_on_protected_areas.sql
│   ├── assert_rating_matches_score.sql
│   ├── assert_geometries_are_valid.sql
│   ├── ...
└── target/
```

## Configuração do projeto

O projeto está configurado em `dbt_project.yml` com:

- nome: `agro_credit_transform`
- profile: `agro_credit_transform`
- paths: `models`, `analyses`, `tests`, `seeds`, `macros`, `snapshots`
- schemas definidos:
  - `agro_esg_staging`
  - `agro_esg_intermediate`
  - `agro_esg_marts`

Também há variáveis globais definidas para regras de negócio e limites técnicos, por exemplo:

- `forest_code_threshold_date`: `2008-07-22`
- `gis_noise_ha_threshold`: `0.01`
- `gis_noise_pct_threshold`: `0.005`
- `fine_deforestation_per_ha`: `5000`
- `fine_protected_area_per_ha`: `10000`
- `fine_slave_labor_fixed`: `500000`
- `fine_app_violation_fixed`: `50000`
- `fine_rl_deficit_per_ha`: `5000`

Esses valores são usados pelo pipeline para decidir limiares de ruído, multas e bloqueios legais.

## Profile e conexão

O profile está em `profiles.yml` e utiliza BigQuery com service account:

- type: `bigquery`
- method: `service-account`
- project: variável `GCP_PROJECT_ID`
- dataset: `agro_esg_staging`
- keyfile: `/home/obscuritenoir/Portfolio/AgroESG-Carbon-Compliance/config/gcp_credentials.json`
- location: `US`

Importante: antes de executar o dbt, certifique-se de que o arquivo de credenciais do GCP exista e a variável de ambiente `GCP_PROJECT_ID` esteja correta.

## Fontes de dados

As fontes brutas são declaradas em `models/staging/sources.yml` e incluem as seguintes categorias:

### Dados de referência e geográficos

- `ibge_biomes`
- `ibge_bc250_rodovias`
- `ibge_bc250_pistas_pouso_l`
- `ibge_bc250_app_zones`
- `ana_app_zones`
- `ana_rios_app`
- `ibge_massas_agua_app`
- `ibge_rodovias_risco`

### Dados de proteção territorial

- `funai_terras_indigenas`
- `incra_quilombolas`
- `incra_assentamentos`
- `intermat_assentamentos`
- `icmbio_unidades_conservacao`
- `sema_unidades_conservacao`

### Dados de risco e compliance

- `ibama_history`
- `sema_mt_embargos`
- `siga_mt_embargos`
- `icmbio_embargos`
- `cadastro_trabalho_escravo`
- `mapbiomas_alertas_shapes`
- `mapbiomas_alertas_car_sigef`
- `sigef_history_mt` / `sigef_history_am` / `sigef_history_ro` / `sigef_history_pa`
- `car_area_imovel_geometria_*`
- `car_temas_ambientais`
- `car_sobreposicao`

## Camadas de modelo

### 1) Staging

Modelos que padronizam as fontes externas e resolvem deduplicação básica.

Exemplos principais:

- `stg_car_properties.sql`
- `stg_car_owners.sql`
- `stg_car_environmental_themes.sql`
- `stg_car_overlaps.sql`
- `stg_ibama.sql`
- `stg_sigef.sql`
- `stg_mte_slave_labor.sql`
- `stg_mapbiomas_alertas.sql`
- `stg_mapbiomas_property_crossings.sql`

Esses modelos muitas vezes fazem:

- renomeação de colunas
- normalização de textos e estados
- deduplicação por `property_id` ou `parcel_id`
- padronização de geometrias em WKT/geometry

### 2) Intermediate

Aqui ficam os modelos que fazem o “coração” do compliance.

Alguns exemplos:

- `int_compliance__property_analysis.sql`: consolida propriedades, risco, embargos, MapBiomas, trabalho escravo, identidade territorial e status de elegibilidade técnico
- `int_property_embargo_overlap.sql`: calcula sobreposição entre imóveis e embargos ambientais
- `int_mapbiomas_deforestation.sql`: consolida alertas de desmatamento e EUDR
- `int_compliance__adjacency_scoring.sql`: mede risco por vizinhança e contaminação de área
- `int_compliance__infrastructure_bridge.sql`: integra evidências de infraestrutura e logística
- `int_compliance_forensic_shapes.sql`: recortes forenses de invasões, APP, embargos e desmatamentos
- `int_brazil_reference_geometries.sql`: geometries de referência para biomas, APP, infraestrutura e áreas protegidas
- `int_car_geometries.sql`: geometria e metadados do CAR
- `int_car_compliance_metrics.sql`: métricas de conformidade do CAR

### 3) Marts

Modelos finais analíticos e de consumo.

Principais modelos:

- `fct_compliance_risk.sql`: tabela final de avaliação de risco e elegibilidade de imóveis
- `fct_property_investment_intelligence.sql`: score de investimento, rating, liquidez e monetização
- `fct_territorial_invasion_forensics.sql`: detalhamento de invasões territoriais
- `fct_social_impact_slave_labor.sql`: impacto social e risco de trabalho escravo
- `fct_compliance_geometries_mart.sql`: mart geoespacial para consumo visual e mapear

## Regras de negócio principais

A lógica de negócio do projeto foi implementada em macros e modelos de alta camada. Alguns aspectos importantes:

### Marco temporal

- Desmatamentos anteriores a `2008-07-22` são ignorados pelo limite legal do Código Florestal
- A regra de EUDR considera `2020-12-31` como corte importante para alertas pós-2020

### Bloqueios em regra

O modelo final considera não elegibilidade em cenários como:

- trabalho escravo com alta confiança
- embargos ambientais ativos
- sobreposição com TI/UC/Quilombo/Assentamento sem identidade legítima
- desmatamento detectado em APP
- desmatamento após corte legal
- inconsistência grave de geometria ou área
- déficit de reserva legal em imóveis sem regra de mitigação

### Identidade territorial

O pipeline resolve casos de identidade patrimonial e territorial, por exemplo:

- imóvel rural privado com sobreposição territorial
- propriedade no CAR que coincida com área indígena e seja legitimamente reconhecida
- resgate de identidade por localização em base de referência territorial

### Risco e mitigação

Há também lógicas para:

- adjacência com propriedades problemáticas
- barreiras físicas (rios, redes, infraestrutura)
- logística e risco estruturado
- risco de contaminação por vizinhança de áreas degradadas

## Macros

O diretório `macros` contém utilitários reutilizáveis:

- `get_compliance_logic.sql`: lógica de cálculo de reserva legal, áreas, status e blocos
- `get_neighbor_barriers_logic.sql`: identifica vizinhos e barreiras físicas entre propriedades
- `sanitize_encoding.sql`: normaliza strings em encoding UTF-8
- `generate_schema_name.sql`: customização de schema dos modelos

## Seeds

A pasta `seeds` contém dados parametrizados e de suporte:

- `legal_parameters.csv`: multas, data limite, limiares e referências legais
- `br_ibge_cities.csv`: cidades do IBGE
- `br_state_codes.csv`: códigos de UF

## Snapshots

Modelos de snapshot para comparar evolução histórica do compliance:

- `compliance_history.sql`
- `snap_all_embargoes.sql`

Esses snapshots ajudam a acompanhar mudança de status ao longo do tempo e auditabilidade da base.

## Testes de qualidade

A pasta `tests` contém validações de integridade de negócio e qualidade dos resultados. Alguns exemplos:

- `assert_embargo_post_2008_blocked.sql`
- `assert_no_eligible_on_social_risk_areas.sql`
- `assert_no_eligible_on_protected_areas.sql`
- `assert_no_negative_areas.sql`
- `assert_no_negative_financial_liability.sql`
- `assert_geometries_are_valid.sql`
- `assert_rating_matches_score.sql`
- `assert_status_matches_hard_block_flags.sql`
- `assert_car_status_is_active.sql`
- `assert_adjacency_risk_flagged_correctly.sql`
- `assert_liability_math_is_correct.sql`

Esses testes reforçam:

- consistência de status
- imóveis não elegíveis em risco crítico
- cálculo de passivos financeiros
- coerência de área e geometria
- integridade da regra de avaliação

## Como executar

### Instalar dependências do dbt

```bash
cd agro_credit_transform

# caso o dbt ainda não esteja instalado no ambiente
pip install dbt-bigquery
# ou use o ambiente do projeto principal
```

### Rodar o projeto

```bash
dbt deps

dbt run
```

### Executar testes

```bash
dbt test
```

### Executar um subset específico de modelos

```bash
dbt run --select fct_compliance_risk

dbt run --select +fct_compliance_risk

dbt test --select assert_no_eligible_on_social_risk_areas
```

### Build completo com testes

```bash
dbt build
```

## Observações de ambiente

- O pipeline foi projetado para rodar em ambiente com BigQuery
- O diretorio `config/` contém credenciais sensíveis e não deve ser versionado sem cuidado
- O profile usa dataset `agro_esg_staging` para metadados e tabelas de output do dbt
- Há uso de geometries espaciais e funções GIS do BigQuery

## Fluxo de valor do projeto

1. Ingestão de dados brutos em `raw_data`
2. Normalização em `staging`
3. Regras espaciais e de negócio em `intermediate`
4. Tabelas analíticas em `marts`
5. Validação com testes dbt
6. Consumo por APIs, BI e processamento downstream

## Dependências principais

- `dbt-core`
- `dbt-bigquery`
- `dbt-utils`

## Resumo

Este diretório encapsula a camada analítica do projeto AgroESG Carbon Compliance. Ele transforma dados públicos e geoespaciais em uma base governada de risco ambiental e elegibilidade, permitindo que a aplicação e o backend tomem decisões sustentadas por evidência técnica, política regulatória e regra de negócio estruturada.

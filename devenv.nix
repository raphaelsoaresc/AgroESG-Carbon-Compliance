{ pkgs, lib, config, inputs, ... }:

{
  # 1. Configurações Gerais
  dotenv.enable = true;
  cachix.enable = false;

  # 2. Variáveis de Ambiente
  env = {
    GREET = "AgroESG ELT Pipeline";
    
    # --- Airflow ---
    AIRFLOW_HOME = "${toString config.env.DEVENV_ROOT}/airflow";
    AIRFLOW__CORE__LOAD_EXAMPLES = "False";
    # Conexão explícita com o Postgres do devenv
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "postgresql+psycopg2://admin:admin123@127.0.0.1:5432/airflow_db";
    AIRFLOW__CORE__EXECUTOR = "LocalExecutor"; # Permite paralelismo real
    
    PYTHONPATH = "${toString config.env.DEVENV_ROOT}";

    # --- DuckDB / GCP ---
    GOOGLE_APPLICATION_CREDENTIALS = "${toString config.env.DEVENV_ROOT}/config/gcp_credentials.json";
  };

  # 3. Python + UV
  languages.python = {
    enable = true;
    version = "3.11";
    uv.enable = true;
    venv.enable = true;
  };

  # 4. Pacotes do Sistema
  packages = with pkgs; [
    duckdb
    google-cloud-sdk
    pkgs.zlib
    stdenv.cc.cc.lib
  ];

  # 5. Serviços (Postgres para o Airflow Metadata)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    listen_addresses = "127.0.0.1";
    initialDatabases = [{ name = "airflow_db"; }];
    initialScript = ''
      DO $$ BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
          CREATE USER admin WITH PASSWORD 'admin123' SUPERUSER;
        END IF;
      END $$;
    '';
  };

  # 6. Scripts Auxiliares
  scripts = {
    setup-project.exec = ''
      echo "🔄 Instalando dependências do pyproject.toml via UV..."
      uv pip install -e .

      echo "🦆 Configurando DuckDB..."
      duckdb -c "INSTALL spatial; INSTALL httpfs;"

      echo "🐘 Inicializando Banco de Dados do Airflow (Postgres)..."
      airflow db migrate

      # Cria usuário apenas se o comando falhar (idempotência)
      airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin || echo "⚠️ Usuário admin já existe ou erro na criação."
      
      echo "✅ Setup concluído!"
    '';

    start-airflow.exec = "airflow standalone";
    
    clean-env.exec = "rm -rf .devenv/state airflow/logs airflow/*.cfg && echo '🗑️ Logs e estados limpos.'";
  };

  # 7. Inicialização Automática
  enterShell = ''
    echo "--------------------------------------------------------"
    echo "🌾 AGRO ESG CARBON COMPLIANCE - AMBIENTE ELT"
    echo "--------------------------------------------------------"
    
    # Cria pasta de estado se não existir
    mkdir -p .devenv/state

    if [ ! -f .devenv/state/setup_done ]; then
      setup-project
      touch .devenv/state/setup_done
    fi
  '';
}
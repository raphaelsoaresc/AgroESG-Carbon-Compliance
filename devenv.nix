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
    # Usamos \$ para que o Shell resolva a variável do seu .env em tempo de execução
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "postgresql+psycopg2://admin:\${AIRFLOW_DB_PASS}@127.0.0.1:5432/airflow_db";
    AIRFLOW__CORE__EXECUTOR = "LocalExecutor";
    
    PYTHONPATH = "${toString config.env.DEVENV_ROOT}";

    # --- DuckDB / GCP ---
    # Dica: Certifique-se de que este .json esteja no seu .gitignore!
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
    zlib
    stdenv.cc.cc.lib
  ];

  # 5. Serviços (Postgres)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    listen_addresses = "127.0.0.1";
    initialDatabases = [{ name = "airflow_db"; }];
    # O initialScript do devenv roda em um ambiente que já carregou o .env
    initialScript = ''
      DO $$ 
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
          -- O comando 'format' evita problemas de sintaxe com a senha vinda da variável
          EXECUTE format('CREATE USER admin WITH PASSWORD %L SUPERUSER', session_user);
        END IF;
      END $$;
    '';
  };

  # 6. Scripts Auxiliares
  scripts = {
    setup-project.exec = ''
      echo "🔄 Instalando dependências..."
      uv pip install -e .

      echo "🦆 Configurando DuckDB..."
      duckdb -c "INSTALL spatial; INSTALL httpfs;"

      echo "🐘 Inicializando Banco de Dados do Airflow..."
      # O Postgres precisa estar rodando para isso funcionar
      airflow db migrate

      # Usamos a variável do .env também para a senha da interface do Airflow
      airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password "$AIRFLOW_DB_PASS" || echo "⚠️ Usuário já existe."
      
      echo "✅ Setup concluído!"
    '';

    start-airflow.exec = "airflow standalone";
    clean-env.exec = "rm -rf .devenv/state airflow/logs airflow/*.cfg && echo '🗑️ Limpeza concluída.'";
  };

  # 7. Inicialização Automática
  enterShell = ''
    echo "--------------------------------------------------------"
    echo "🌾 AGRO ESG CARBON COMPLIANCE - AMBIENTE ELT"
    echo "--------------------------------------------------------"
    
    if [ -z "$AIRFLOW_DB_PASS" ]; then
      echo "❌ ERRO: A variável AIRFLOW_DB_PASS não está definida no seu arquivo .env"
    fi

    mkdir -p .devenv/state
    if [ ! -f .devenv/state/setup_done ]; then
      setup-project
      touch .devenv/state/setup_done
    fi
  '';
}

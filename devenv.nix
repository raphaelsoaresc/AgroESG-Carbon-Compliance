{ pkgs, lib, config, inputs, ... }:

{
  # 1. Configurações Gerais
  dotenv.enable = true;
  cachix.enable = false;

  # 2. Variáveis de Ambiente
  env = {
    GREET = "AgroESG Project (Stealth Mode)";
    
    # --- Airflow ---
    AIRFLOW_HOME = "${toString config.env.DEVENV_ROOT}/airflow";
    AIRFLOW__CORE__LOAD_EXAMPLES = "False";
    PYTHONPATH = "${toString config.env.DEVENV_ROOT}:${toString config.env.DEVENV_ROOT}/airflow/dags";

    # --- DuckDB / GCP ---
    DUCKDB_GCS_KEYFILE = "${toString config.env.GOOGLE_APPLICATION_CREDENTIALS}";
    
    # --- Proxy (Tor) ---
    HTTP_PROXY = "socks5h://127.0.0.1:9050";
    HTTPS_PROXY = "socks5h://127.0.0.1:9050";
    
    # [CRUCIAL] Exceções que NÃO passam pelo Tor
    NO_PROXY = "localhost,127.0.0.1,::1,.googleapis.com,.google.com,metadata.google.internal";
  };

  # 3. Python + UV
  languages.python = {
    enable = true;
    version = "3.11";
    uv.enable = true;
    venv = {
      enable = true;
      quiet = false;
    };
  };

  # 4. Pacotes
  packages = with pkgs; [
    gdal geos proj                # Geoespacial
    google-cloud-sdk duckdb       # Dados
    tor proxychains-ng curl       # Rede/Anonimato
    git openssl libiconv stdenv.cc.cc.lib # Compilação
  ];

  # 5. Serviços (Postgres)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    extensions = ps: [ ps.postgis ]; 
    listen_addresses = "127.0.0.1"; 
    initialDatabases = [{ name = "agro_risk"; }];
    initialScript = ''
      DO $$ BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
          CREATE USER admin WITH PASSWORD 'admin123' SUPERUSER;
        END IF;
      END $$;
      GRANT ALL PRIVILEGES ON DATABASE agro_risk TO admin;
    '';
  };

  # 6. Processos (Tor)
  processes = {
    tor.exec = "mkdir -p .devenv/state/tor && ${pkgs.tor}/bin/tor --SocksPort 9050 --DataDirectory .devenv/state/tor --MaxCircuitDirtiness 600";
  };

  # 7. Scripts Auxiliares
  scripts = {
    # Script Manual (Backup importante caso o automático falhe)
    setup-project.exec = ''
      echo "🔄 Forçando reinstalação manual..."
      unset HTTP_PROXY HTTPS_PROXY NO_PROXY
      export AIRFLOW_VERSION=2.10.0
      export PYTHON_VERSION=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
      export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-''${AIRFLOW_VERSION}/constraints-''${PYTHON_VERSION}.txt"

      uv pip install \
        "apache-airflow[google,pandas]==''${AIRFLOW_VERSION}" \
        "dbt-bigquery" "astronomer-cosmos" "duckdb" \
        "geopandas" "shapely" "pyarrow" \
        "requests[socks]" "fake-useragent" "cloudscraper" \
        "google-cloud-storage" \
        --constraint "''${CONSTRAINT_URL}"
      
      airflow db migrate
      # Cria o arquivo de controle para o enterShell saber que já foi feito
      mkdir -p .devenv/state && touch .devenv/state/airflow-installed
      echo "✅ Setup manual concluído!"
    '';

    # Script para limpar tudo e forçar reinstalação no próximo login
    force-reinstall.exec = "rm -f .devenv/state/airflow-installed && echo '🗑️ Estado limpo. Reinicie o shell (exit + devenv shell) para reinstalar.'";

    check-connection.exec = ''
      echo "--- TESTE DE ROTEAMENTO ---"
      echo "1. IP via Tor (Deve ser diferente do real):"
      curl -s https://icanhazip.com
      echo "2. IP Direto (Deve ser o seu IP real):"
      curl -s --noproxy "*" https://icanhazip.com
    '';

    start-airflow.exec = "airflow standalone";
  };

  # 8. Inicialização Automática
  enterShell = ''
    echo "--------------------------------------------------------"
    echo "🕵️  AMBIENTE HÍBRIDO: SCRAPING (TOR) + UPLOAD (DIRECT)"
    echo "--------------------------------------------------------"
    
    # Validações de Ambiente
    if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
      echo "⚠️  GCP: Credenciais não encontradas no .env"
    else
      echo "☁️  GCP: Chave carregada."
    fi

    # (Recuperado do arquivo 2) Aviso importante do Bucket
    if [ -z "$GCS_LANDING_BUCKET" ]; then
      echo "⚠️  DICA: Adicione GCS_LANDING_BUCKET ao seu .env"
    fi

    # --- AUTO-INSTALL DO AIRFLOW ---
    if [ ! -f .devenv/state/airflow-installed ]; then
      echo "🚀 Detectado ambiente novo. Instalando Airflow Full Stack..."
      
      # Desativa proxy temporariamente para baixar rápido
      unset HTTP_PROXY HTTPS_PROXY NO_PROXY
      
      export AIRFLOW_VERSION=2.10.0
      export PYTHON_VERSION=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
      # URL Corrigida (estava quebrada no snippet anterior)
      export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-''${AIRFLOW_VERSION}/constraints-''${PYTHON_VERSION}.txt"

      echo "📦 Baixando pacotes (aguarde)..."
      uv pip install \
        "apache-airflow[google,pandas]==''${AIRFLOW_VERSION}" \
        "dbt-bigquery" "astronomer-cosmos" "duckdb" \
        "geopandas" "shapely" "pyarrow" \
        "requests[socks]" "fake-useragent" "cloudscraper" \
        "google-cloud-storage" \
        --constraint "''${CONSTRAINT_URL}"
      
      if [ $? -eq 0 ]; then
        echo "⚙️  Inicializando banco de dados do Airflow..."
        airflow db migrate
        mkdir -p .devenv/state
        touch .devenv/state/airflow-installed
        echo "✅ Instalação concluída com sucesso!"
      else
        echo "❌ Falha na instalação. Tente rodar 'setup-project' manualmente."
      fi
    else
      echo "✅ Airflow já instalado."
    fi
    # --- Fim do Auto-Install ---

    echo "🧅 Proxy Tor configurado."
    echo "⚠️  Lembre-se: 'devenv up' em outra aba para rodar Tor/Postgres."
  '';
}
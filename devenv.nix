{ pkgs, lib, config, inputs, ... }:

{
  # 1. Configurações Gerais
  dotenv.enable = true;
  cachix.enable = false;

  # 2. Variáveis de Ambiente
  env = {
    GREET = "AgroESG ELT Pipeline";
    
    # --- LD_LIBRARY_PATH: Nomes corrigidos para o Nixpkgs ---
    LD_LIBRARY_PATH = lib.makeLibraryPath [
      pkgs.stdenv.cc.cc.lib
      pkgs.expat
      pkgs.gdal
      pkgs.geos      # Corrigido: No Nix é 'geos', não 'libgeos'
      pkgs.proj
      pkgs.zlib
      pkgs.libtiff
      pkgs.libjpeg
      pkgs.libpng
    ];
    
    # --- Airflow ---
    AIRFLOW_HOME = "${toString config.env.DEVENV_ROOT}/airflow";
    AIRFLOW__CORE__LOAD_EXAMPLES = "False";
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "postgresql+psycopg2://admin:\${AIRFLOW_DB_PASS}@127.0.0.1:5432/airflow_db";
    AIRFLOW__CORE__EXECUTOR = "LocalExecutor";
    
    PYTHONPATH = "${toString config.env.DEVENV_ROOT}";

    # --- DuckDB / GCP ---
    GOOGLE_APPLICATION_CREDENTIALS = "${toString config.env.DEVENV_ROOT}/config/gcp_credentials.json";
  };

  # 3. Linguagens
  languages.python = {
    enable = true;
    version = "3.11";
    uv.enable = true;
    venv.enable = true;
  };

  languages.javascript = {
    enable = true;
    package = pkgs.nodejs_20; 
    npm.enable = true;
  };

  # 4. Processos (Gerenciados pelo 'devenv up')
  processes = {
    frontend.exec = "cd caipora-frontend && npm run dev";
  };

  # 5. Pacotes do Sistema (Nomes validados para Nix)
  packages = with pkgs; [
    duckdb
    google-cloud-sdk
    zlib
    stdenv.cc.cc.lib
    # Dependências para GIS/Satélite
    expat
    gdal
    geos   # Corrigido aqui também
    proj
    libtiff
    libjpeg
    libpng
  ];

  # 6. Serviços (Postgres)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    listen_addresses = "127.0.0.1";
    initialDatabases = [{ name = "airflow_db"; }];
    initialScript = ''
      DO $$ 
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
          EXECUTE format('CREATE USER admin WITH PASSWORD %L SUPERUSER', session_user);
        END IF;
      END $$;
    '';
  };

  # 7. Scripts Auxiliares
  scripts = {
    setup-project.exec = ''
      echo "📦 Instalando dependências do Frontend (Next.js + Tailwind v4)..."
      cd caipora-frontend && npm install tailwindcss@next @tailwindcss/postcss@next postcss
      cd ..

      echo "🔄 Instalando dependências Python..."
      uv pip install -e .

      echo "🦆 Configurando DuckDB..."
      duckdb -c "INSTALL spatial; INSTALL httpfs;"

      echo "🐘 Inicializando Banco de Dados do Airflow..."
      airflow db migrate

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

  # 8. Inicialização Automática
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
{ pkgs, lib, config, inputs, ... }:

{
  # Variáveis de ambiente
  env.GREET = "AgroESG Project";

  # 1. Ativação de Linguagens (Recomendado para usar com uv)
  languages.python = {
    enable = true;
    uv.enable = true;
    # Cria o venv automaticamente se não existir
    venv.enable = true; 
  };

  # 2. Pacotes do Sistema (binários que você usa no terminal)
  packages = [
    pkgs.duckdb  # O motor do banco de dados
    pkgs.git
  ];

  # 3. Serviços (PostgreSQL com PostGIS para dados geoespaciais do IBAMA)
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    extensions = ps: [ ps.postgis ]; 
    listen_addresses = "127.0.0.1"; 
    initialDatabases = [
      { name = "agro_risk"; }
    ];
    
    initialScript = ''
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin') THEN
          CREATE USER admin WITH PASSWORD 'admin123' SUPERUSER;
        END IF;
      END
      $$;
      GRANT ALL PRIVILEGES ON DATABASE agro_risk TO admin;
    '';
  };

  # 4. Scripts auxiliares
  scripts.hello.exec = "echo Bem-vindo ao $GREET";

  # 5. Comandos executados ao entrar no terminal (devenv shell)
  enterShell = ''
    hello
    echo "Python version: $(python --version)"
    echo "DuckDB version: $(duckdb --version)"
    # Dica: uv sync garante que as libs python (dbt, duckdb-python) estejam instaladas
    uv sync
  '';

  # 6. Testes básicos de sanidade
  enterTest = ''
    echo "Running environment checks..."
    duckdb --version
    uv --version
  '';

  dotenv.enable = true;

  # Desativa o cachix se não estiver usando
  cachix.enable = false;
}

FROM python:3.11-slim

# Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PORT 8080

WORKDIR /app

# Instala dependências de sistema necessárias para compilação e DuckDB
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências do Python
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- MELHORIA DE PERFORMANCE (SEM ALTERAR REGRAS) ---
# Criamos o diretório de extensões e pré-instalamos o 'spatial' 
# Isso evita que o container tente baixar isso durante o boot, economizando tempo crítico.
RUN mkdir -p /tmp/duckdb_extensions && \
    python -c "import duckdb; con = duckdb.connect(':memory:'); con.execute(\"SET extension_directory='/tmp/duckdb_extensions';\"); con.execute('INSTALL spatial;')"

# Copia todo o código do projeto
COPY . .

# O Cloud Run usa a porta 8080 por padrão
EXPOSE 8080

# Comando para rodar a aplicação
# Usamos o exec para que o sinal de encerramento (SIGTERM) seja propagado corretamente
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT}
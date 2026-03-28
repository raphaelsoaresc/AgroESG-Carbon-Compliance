FROM python:3.11-slim

WORKDIR /app

# Instala dependências de sistema
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências do Python
COPY requirements.api.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# --- A CORREÇÃO ESTÁ AQUI ---
# Em vez de copiar arquivo por arquivo, copia tudo (pastas routers, services, etc.)
COPY . .
# ----------------------------

# O Cloud Run ignora o EXPOSE, mas é boa prática manter
EXPOSE 8080

# Garanta que o nome do arquivo seja main.py (conforme seu CMD) ou app.py (conforme sua imagem)
# Se o arquivo principal for app.py, mude para "app:app"
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}

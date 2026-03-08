FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# --- MUDANÇA AQUI ---
# Copia o requirements da API e renomeia para requirements.txt dentro do container
COPY requirements.api.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
# --------------------

COPY main.py .
COPY schemas.py .

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
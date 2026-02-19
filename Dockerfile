FROM python:3.9-slim

WORKDIR /app

# Copia os requerimentos e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# COPIA O ARQUIVO PARQUET E O APP
# (Certifique-se de ter rodado o script de exportação antes do docker build)
COPY data_compliance.parquet .
COPY app.py .

EXPOSE 8501

CMD ["sh", "-c", "streamlit run app.py --server.port=$PORT --server.address=0.0.0.0"]
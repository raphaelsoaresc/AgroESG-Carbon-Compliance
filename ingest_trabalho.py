import os
import hashlib
import pandas as pd
import pdfplumber
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

def get_file_hash(file_path):
    """Gera um hash MD5 do arquivo para rastreabilidade."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def ingest_trabalho_escravo_to_bq():
    # Configurações do BigQuery
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BQ_DATASET_ID")
    TABLE_ID = "cadastro_trabalho_escravo"
    
    BASE_RAW_PATH = os.getenv("RAW_PATH_TRABALHO", "./data/raw/trabalho").strip('"')
    
    client = bigquery.Client(project=PROJECT_ID)
    full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    all_rows = []
    ingested_at = datetime.utcnow() # Timestamp único para o lote

    # Lista arquivos PDF
    pdf_files = [f for f in os.listdir(BASE_RAW_PATH) if f.endswith('.pdf')]
    
    if not pdf_files:
        print(f"❌ Nenhum arquivo PDF encontrado em: {BASE_RAW_PATH}")
        return

    for file_name in pdf_files:
        path = os.path.join(BASE_RAW_PATH, file_name)
        print(f"📄 Processando arquivo: {file_name}")
        
        # Gera hash do arquivo
        file_hash = get_file_hash(path)
        
        with pdfplumber.open(path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                table = page.extract_table()
                
                if not table:
                    continue

                for row in table:
                    # Validação: O primeiro campo (ID) deve ser preenchido e numérico
                    if not row[0] or not str(row[0]).strip().isdigit():
                        continue
                    
                    # Limpeza básica: remove quebras de linha internas
                    clean_row = [str(cell).replace('\n', ' ').strip() if cell else "" for cell in row]

                    # Mapeamento com os nomes exatos solicitados
                    all_rows.append({
                        "id": clean_row[0],
                        "ano_da_acao_fiscal": clean_row[1],
                        "uf": clean_row[2],
                        "empregador": clean_row[3],
                        "cnpjcpf": clean_row[4],
                        "estabelecimento": clean_row[5],
                        "trabalhadores_envolvidos": clean_row[6],
                        "cnae": clean_row[7],
                        "decisao_administrativa_de_procedencia": clean_row[8],
                        "inclusao_no_cadastro_de_empregadores": clean_row[9],
                        # Colunas de Metadados
                        "file_hash": file_hash,
                        "source_filename": file_name,
                        "ingested_at": ingested_at
                    })
                
                print(f"✅ Página {page_num + 1} extraída.")

    if not all_rows:
        print("⚠️ Nenhum dado extraído.")
        return

    # Cria o DataFrame (tudo como string, exceto ingested_at)
    df = pd.DataFrame(all_rows)

    # Configuração do Schema conforme sua solicitação
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("ano_da_acao_fiscal", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("empregador", "STRING"),
            bigquery.SchemaField("cnpjcpf", "STRING"),
            bigquery.SchemaField("estabelecimento", "STRING"),
            bigquery.SchemaField("trabalhadores_envolvidos", "STRING"),
            bigquery.SchemaField("cnae", "STRING"),
            bigquery.SchemaField("decisao_administrativa_de_procedencia", "STRING"),
            bigquery.SchemaField("inclusao_no_cadastro_de_empregadores", "STRING"),
            bigquery.SchemaField("file_hash", "STRING"),
            bigquery.SchemaField("source_filename", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
    )

    print(f"📤 Subindo {len(df)} linhas para BigQuery...")
    
    try:
        job = client.load_table_from_dataframe(df, full_table_path, job_config=job_config)
        job.result()
        print(f"🚀 Ingestão concluída com sucesso na tabela: {TABLE_ID}")
    except Exception as e:
        print(f"❌ Erro na carga do BigQuery: {e}")

if __name__ == "__main__":
    ingest_trabalho_escravo_to_bq()
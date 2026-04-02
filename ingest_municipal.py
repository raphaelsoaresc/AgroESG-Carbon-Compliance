import os
import re
import pandas as pd
import pdfplumber
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def ingest_incra_pdf_to_bq():
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("BQ_DATASET_ID")
    TABLE_ID = "incra_municipal_indices"
    
    BASE_RAW_PATH = os.getenv("RAW_PATH_INCRA", "./data/raw/incra").strip('"')
    
    client = bigquery.Client(project=PROJECT_ID)
    full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    all_data = []

    # Regex para capturar a linha de dados:
    # Grupo 1: Código IBGE (7 dígitos)
    # Grupo 2: Nome do Município
    # Grupo 3: MRG (3 dígitos)
    # Grupo 4: ZP
    # Grupo 5: Módulo Fiscal (ha)
    # Grupo 6: ZTM
    # Grupo 7: FMP (ha)
    pattern = re.compile(r'^(\d{7})\s+(.*?)\s+(\d{3})\s+(\d+)\s+(\d+)\s+(\S+)\s+(\d+)')

    pdf_files = [f for f in os.listdir(BASE_RAW_PATH) if f.endswith('.pdf')]
    
    for file in pdf_files:
        path = os.path.join(BASE_RAW_PATH, file)
        print(f"Processando PDF via Texto: {file}")
        
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                
                for line in text.split('\n'):
                    match = pattern.match(line.strip())
                    if match:
                        all_data.append({
                            "municipio_id": match.group(1),
                            "municipio_nome": match.group(2).strip(),
                            "uf_sigla": match.group(1)[:2], # Primeiros 2 dígitos do IBGE
                            "modulo_fiscal_ha": float(match.group(5)),
                            "fmp_ha": float(match.group(7))
                        })

    if not all_data:
        print("⚠️ Nenhum dado foi extraído. Verifique o padrão do PDF.")
        return

    df = pd.DataFrame(all_data)
    print(f"✅ Sucesso! {len(df)} municípios extraídos.")

    # Mapeamento de UFs (Opcional, mas ajuda a deixar o dado limpo)
    # O IBGE usa: 15=PA, 23=CE, 26=PE, 52=GO, 29=BA, 31=MG, 33=RJ, 35=SP, 41=PR, 42=SC, 11=RO, 14=RR, 17=TO, 13=AM, 16=AP, 25=PB, 28=SE, 50=MS, 32=ES, 21=MA, 24=PI
    
    # Carga para o BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("municipio_id", "STRING"),
            bigquery.SchemaField("municipio_nome", "STRING"),
            bigquery.SchemaField("uf_sigla", "STRING"),
            bigquery.SchemaField("modulo_fiscal_ha", "FLOAT"),
            bigquery.SchemaField("fmp_ha", "FLOAT"),
        ]
    )

    print(f"Subindo para BigQuery: {full_table_path}...")
    client.load_table_from_dataframe(df, full_table_path, job_config=job_config).result()
    print("🚀 Ingestão concluída com sucesso.")

if __name__ == "__main__":
    ingest_incra_pdf_to_bq()
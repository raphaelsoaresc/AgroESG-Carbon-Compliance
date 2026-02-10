import os
import geopandas as gpd
import pandas as pd
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID")
SHAPEFILE_PATH = os.getenv("SIGEF_PATH")
# Mudamos o nome para _raw para indicar que é dado bruto
# We changed the name to _raw to indicate it is raw data
TABLE_ID = "sigef_mt_raw" 

if not all([PROJECT_ID, DATASET_ID, SHAPEFILE_PATH]):
    raise ValueError("Erro: Verifique as variáveis de ambiente.")

def load_sigef_raw():
    print(f"--- INGESTÃO RAW: SIGEF MT ---")
    print(f"Lendo arquivo: {SHAPEFILE_PATH}")

    # 1. Leitura / Reading
    gdf = gpd.read_file(Path(SHAPEFILE_PATH))
    print(f"Linhas lidas: {len(gdf)}")
    print(f"CRS Original detectado: {gdf.crs}") 

    # 2. Preparação / Preparation
    df = pd.DataFrame(gdf.drop(columns='geometry'))
    df['wkt_geometry'] = gdf.geometry.to_wkt()
    
    # Convert everything to string to ensure integrity
    df = df.astype(str)

    # 3. Carga / Load
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        schema=[
            bigquery.SchemaField("wkt_geometry", "STRING"),
        ]
    )

    print(f"Carregando para {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f"✅ Sucesso! Tabela {TABLE_ID} carregada.")

if __name__ == "__main__":
    load_sigef_raw()
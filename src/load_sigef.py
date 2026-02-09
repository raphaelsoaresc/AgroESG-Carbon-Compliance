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
TABLE_ID = os.getenv("TABLE_ID")

if not all([PROJECT_ID, DATASET_ID, SHAPEFILE_PATH, TABLE_ID]):
    raise ValueError("Erro: Verifique se GCP_PROJECT_ID, BQ_DATASET_ID, SIGEF_PATH e TABLE_ID estão no .env")


def load_sigef():
    print(f"--- Iniciando Carga SIGEF para {PROJECT_ID}.{DATASET_ID} ---")
    print(f"Lendo arquivo: {SHAPEFILE_PATH}")

    gdf = gpd.read_file(Path(SHAPEFILE_PATH))

    print(f"Linhas lidas: {len(gdf)}")

    if gdf.crs != "EPSG:4326":
        print(f"Convertendo CRS de {gdf.crs} para EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    df = pd.DataFrame(gdf.drop(columns='geometry'))
    df['wkt_geometry'] = gdf.geometry.to_wkt()

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        schema=[
            bigquery.SchemaField("wkt_geometry", "STRING"),
        ]
    )

    print(f"Carregando dados para o BigQuery...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    job.result()

    print(f"Sucesso! Tabela {table_ref} carregada.")

if __name__ == "__main__":
    load_sigef()
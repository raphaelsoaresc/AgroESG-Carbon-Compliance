from google.cloud import bigquery
import pandas as pd
import geopandas as gpd
from shapely import wkt
import os

def export():
    client = bigquery.Client()
    
    # Certifique-se de que sua query traz a coluna de geometria (ex: 'geometry')
    query = "SELECT * FROM `agroesg-carbon-compliance.agro_esg_marts.fct_compliance_risk`"
    
    print("🛰️ Baixando dados do BigQuery...")
    df = client.query(query).to_dataframe()

    # --- CONVERSÃO PARA GEOPANDAS ---
    # Substitua 'geometry' pelo nome real da sua coluna de coordenadas, se for diferente
    if 'geometry' in df.columns:
        print("🗺️ Convertendo para GeoDataFrame...")
        # Converte a string WKT do BigQuery para objetos reais de geometria
        df['geometry'] = df['geometry'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    else:
        raise ValueError("Coluna 'geometry' não encontrada no resultado da query.")

    # --- LIMPEZA E OTIMIZAÇÃO ---
    for col in gdf.columns:
        if str(gdf[col].dtype) == 'dbdate':
            gdf[col] = pd.to_datetime(gdf[col])

    # --- EXPORTAÇÃO GEOPARQUET ---
    print(f"✅ Exportando {len(gdf):,} linhas para GeoParquet...")
    gdf.to_parquet('data_compliance.geoparquet', index=False, compression='snappy')
    print("🚀 Sucesso! Arquivo 'data_compliance.geoparquet' pronto.")

if __name__ == "__main__":
    export()

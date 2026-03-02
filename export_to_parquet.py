from google.cloud import bigquery
import pandas as pd
import json

def export():
    client = bigquery.Client()
    
    # 1. SQL Otimizado: Simplificamos a geometria no BigQuery (0.001 é um ótimo balanço)
    # 2. Convertemos para GeoJSON direto no SQL para evitar processamento lento no Python
    query = """
    SELECT 
        * EXCEPT(geom_json),
        ST_ASGEOJSON(ST_SIMPLIFY(ST_GEOGFROMGEOJSON(geom_json), 0.001)) as geom_json
    FROM `agroesg-carbon-compliance.agro_esg_marts.fct_compliance_risk`
    """
    
    print("🛰️ Baixando e simplificando dados no BigQuery...")
    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    # --- LIMPEZA DE TIPOS ---
    for col in df.columns:
        # Resolve o erro 'dbdate' convertendo para datetime padrão
        if str(df[col].dtype) == 'dbdate':
            df[col] = pd.to_datetime(df[col])
        
        # Remove colunas de sistema do BigQuery (geography) se existirem
        if str(df[col].dtype) in ['geometry', 'geography']: 
            df = df.drop(columns=[col])

    # --- OTIMIZAÇÃO DE MEMÓRIA (Essencial para Mobile) ---
    # Downcast de floats para float32 (reduz 50% da RAM das colunas numéricas)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')

    # Transforma textos repetitivos em categorias (Economiza ~80% de RAM em biomas/status)
    for col in ['biome_name', 'eligibility_status']:
        if col in df.columns:
            df[col] = df[col].astype('category')

    print(f"✅ Exportando {len(df):,} linhas otimizadas...")
    df.to_parquet('data_compliance.parquet', index=False, engine='pyarrow', compression='snappy')
    print("🚀 Sucesso! Arquivo 'data_compliance.parquet' pronto.")

if __name__ == "__main__":
    export()

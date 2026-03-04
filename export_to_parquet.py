from google.cloud import bigquery
import pandas as pd
import os

def export():
    client = bigquery.Client()
    
    # SQL Direto: Pegamos tudo da tabela final
    query = "SELECT * FROM `agroesg-carbon-compliance.agro_esg_marts.fct_compliance_risk`"
    
    print("🛰️ Baixando dados de auditoria do BigQuery...")
    df = client.query(query).to_dataframe(create_bqstorage_client=False)

    # --- LIMPEZA DE TIPOS ---
    for col in df.columns:
        # Resolve o erro 'dbdate' do BigQuery
        if str(df[col].dtype) == 'dbdate':
            df[col] = pd.to_datetime(df[col])

    # --- OTIMIZAÇÃO DE MEMÓRIA ---
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')

    for col in ['biome_name', 'final_eligibility_status', 'car_status']:
        if col in df.columns:
            df[col] = df[col].astype('category')

    print(f"✅ Exportando {len(df):,} linhas para o Streamlit...")
    df.to_parquet('data_compliance.parquet', index=False, engine='pyarrow', compression='snappy')
    print("🚀 Sucesso! Arquivo 'data_compliance.parquet' pronto.")

if __name__ == "__main__":
    export()
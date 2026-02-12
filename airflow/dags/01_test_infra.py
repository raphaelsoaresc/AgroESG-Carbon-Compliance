from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import duckdb
import os

# Definição padrão do DAG
default_args = {
    'owner': 'AgroESG',
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

def check_tor_ip():
    """Verifica se o Airflow está usando o Proxy Tor"""
    print("--- TESTE DE CONEXÃO TOR ---")
    try:
        # O requests vai ler as variáveis HTTP_PROXY do sistema automaticamente
        response = requests.get('https://icanhazip.com', timeout=10)
        ip = response.text.strip()
        print(f"Meu IP visto pela internet é: {ip}")
        
        # Validação simples (se o IP não for o seu 181..., funcionou)
        if not ip.startswith("181."): 
            print("✅ SUCESSO: Estou navegando anonimamente!")
        else:
            print("⚠️ AVISO: Estou usando o IP real.")
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")

def check_duckdb():
    """Testa se o DuckDB consegue criar tabelas e processar dados"""
    print("--- TESTE DUCKDB ---")
    try:
        # Cria um banco em memória
        con = duckdb.connect(database=':memory:')
        
        # Cria uma tabela simples e faz uma query
        con.execute("CREATE TABLE agro_test (id INTEGER, cultura VARCHAR)")
        con.execute("INSERT INTO agro_test VALUES (1, 'Soja'), (2, 'Milho')")
        result = con.execute("SELECT * FROM agro_test WHERE cultura = 'Soja'").fetchall()
        
        print(f"Resultado do DuckDB: {result}")
        if result == [(1, 'Soja')]:
            print("✅ SUCESSO: DuckDB está processando dados!")
        else:
            print("❌ Erro: Resultado inesperado.")
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")

with DAG('01_infra_check', default_args=default_args, schedule_interval=None) as dag:
    
    t1 = PythonOperator(
        task_id='verificar_tor',
        python_callable=check_tor_ip
    )

    t2 = PythonOperator(
        task_id='verificar_duckdb',
        python_callable=check_duckdb
    )

    t1 >> t2
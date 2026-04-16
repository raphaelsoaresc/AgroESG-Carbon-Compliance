import requests
import pandas as pd
from fpdf import FPDF
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime
import sys
import os

# --- CONFIGURAÇÃO DO CLIENTE ---
class ClientSettings(BaseSettings):
    api_base_url: str
    api_password: str
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

config = ClientSettings()

class PDFReport(FPDF):
    def __init__(self, car_code):
        super().__init__()
        self.car_code = car_code

    def header(self):
        # Logo ou Título
        self.set_font("Arial", "B", 14)
        self.cell(0, 10, f"Relatório de Compliance - CAR: {self.car_code}", ln=True, align="C")
        self.set_font("Arial", "I", 10)
        self.cell(0, 5, "Caipora Sentinela - Monitoramento Geoespacial", ln=True, align="C")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Arial", "I", 8)
        self.cell(0, 10, f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')} - Página {self.page_no()}", align="C")

def get_data_by_car(car_code: str):
    """Consome a API filtrando por um CAR específico"""
    # Ajuste o endpoint conforme definido no seu router (ex: /compliance?car_code=...)
    endpoint = "/compliance" 
    url = f"{config.api_base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    
    headers = {"X-API-Key": config.api_password}
    params = {"car_code": car_code} # Enviando o filtro
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print(f"⚠️ Nenhum registro encontrado para o CAR: {car_code}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao chamar API: {e}")
        sys.exit(1)

def generate_pdf(data, car_code):
    """Gera o PDF formatado para um único CAR"""
    # Se a API retornar um dicionário único, transformamos em lista para o DataFrame
    if isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = pd.DataFrame(data)

    pdf = PDFReport(car_code)
    pdf.add_page()
    
    # --- SEÇÃO DE RESUMO ---
    pdf.set_font("Arial", "B", 12)
    pdf.set_fill_color(230, 230, 230)
    pdf.cell(0, 10, " Detalhes do Imóvel", ln=True, fill=True)
    pdf.ln(2)
    
    pdf.set_font("Arial", size=10)
    # Itera sobre as colunas para criar uma lista vertical (melhor para um único registro)
    for col in df.columns:
        valor = str(df[col].iloc[0])
        # Evita imprimir geometrias gigantes no PDF
        if "geometry" in col.lower() or "geom" in col.lower():
            continue
        pdf.set_font("Arial", "B", 10)
        pdf.write(8, f"{col.upper()}: ")
        pdf.set_font("Arial", "", 10)
        pdf.write(8, f"{valor}\n")
    
    # Salva o arquivo com o nome do CAR
    filename = f"Relatorio_{car_code.replace('/', '_')}.pdf"
    pdf.output(filename)
    print(f"✅ PDF '{filename}' gerado com sucesso!")

if __name__ == "__main__":
    # Verifica se o código CAR foi passado via argumento de linha de comando
    # Exemplo: python generate_report.py MT-5107909-6E6...
    if len(sys.argv) < 2:
        print("❌ Erro: Você precisa informar o código CAR.")
        print("Uso: python generate_report.py CODIGO_DO_CAR")
        sys.exit(1)
    
    target_car = sys.argv[1]
    
    print(f"📡 Consultando dados para o CAR: {target_car}...")
    dados = get_data_by_car(target_car)
    
    if dados:
        print("📄 Criando arquivo PDF...")
        generate_pdf(dados, target_car)
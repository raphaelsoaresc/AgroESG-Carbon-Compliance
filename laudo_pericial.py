import os
import io
import logging
from datetime import datetime
from dotenv import load_dotenv

# --- GOOGLE CLOUD CLIENTS ---
from google.cloud import bigquery
from google.cloud import storage

# --- BIBLIOTECAS DE ELITE ---
from fpdf import FPDF
import matplotlib.pyplot as plt
import contextily as ctx
import geopandas as gpd
from shapely import wkt

# Configuração de Logging para ver o que está acontecendo no terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CARREGAMENTO DO AMBIENTE ---
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID_MARTS")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

# --- CLASSE DO LAUDO (DESIGN MODERNO) ---
class CaiporaReport(FPDF):
    def __init__(self, status_cor=(39, 174, 96)):
        super().__init__()
        self.status_cor = status_cor

    def header(self):
        # Tarja de Topo Dinâmica
        self.set_fill_color(*self.status_cor)
        self.rect(0, 0, 210, 35, 'F')
        self.set_text_color(255, 255, 255)
        self.set_font('helvetica', 'B', 16)
        self.cell(0, 15, 'CAIPORA SENTINELA - COMPLIANCE GEOESPACIAL', align='C', new_x="LMARGIN", new_y="NEXT")
        self.set_font('helvetica', '', 10)
        self.cell(0, 5, 'PARECER TÉCNICO DE CONFORMIDADE SOCIOAMBIENTAL', align='C', new_x="LMARGIN", new_y="NEXT")
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Página {self.page_no()} | Documento Gerado via Geointeligência Automatizada', align='C')

    def section_title(self, label):
        self.set_font('helvetica', 'B', 11)
        self.set_fill_color(230, 230, 230)
        self.set_text_color(44, 62, 80)
        self.cell(0, 8, f' {label}', fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def field(self, label, value, alert=False):
        """Imprime um campo com rótulo em negrito. Se alert=True, o texto fica vermelho."""
        self.set_font('helvetica', 'B', 9)
        if alert:
            self.set_text_color(200, 0, 0)
        else:
            self.set_text_color(50, 50, 50)
        
        self.write(5, f"{label}: ")
        self.set_font('helvetica', '' if not alert else 'B', 9)
        self.write(5, f"{value}\n")
        self.set_text_color(50, 50, 50) # Reset cor

# --- FUNÇÃO GERADORA DE MAPA ---
def generate_map(geometry_wkt):
    try:
        poly = wkt.loads(geometry_wkt)
        gdf_map = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[poly])
        gdf_map = gdf_map.to_crs(epsg=3857)
        fig, ax = plt.subplots(figsize=(10, 6))
        gdf_map.plot(ax=ax, facecolor="none", edgecolor="#c0392b", linewidth=3)
        ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, attribution="")
        ax.set_axis_off()
        img_buf = io.BytesIO()
        plt.savefig(img_buf, format='png', bbox_inches='tight', dpi=150)
        plt.close(fig)
        img_buf.seek(0)
        return img_buf
    except Exception as e:
        logging.error(f"Erro ao gerar mapa: {e}")
        return None

# --- MOTOR DE PROCESSAMENTO ---
def run_report():
    logging.info("🚀 Iniciando Motor de Geração de Laudos Caipora...")
    
    client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Query para buscar o exemplo do PA e amostras do MT
    sql = f"""
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.fct_compliance_risk` 
    WHERE property_id = 'PA-1505031-538B86A241DA4C44BC7C1C093D3D85F3'
       OR (uf_origem = 'MT' AND is_technically_blocked = TRUE)
    LIMIT 5
    """
    
    logging.info("🔍 Consultando BigQuery...")
    query_job = client.query(sql)
    properties = [dict(row) for row in query_job.result()]

    if not properties:
        logging.warning("⚠️ Nenhuma propriedade encontrada na consulta. Verifique os filtros do SQL.")
        return

    # Pasta local para salvar os arquivos antes do upload
    os.makedirs("laudos_finais", exist_ok=True)

    for p in properties:
        logging.info(f"📄 Processando: {p['property_alias']} ({p['property_id']})")

        # --- MOTOR DE DECISÃO (LÓGICA PERICIAL) ---
        # Definimos se o laudo é Vermelho (Bloqueado) ou Verde (Elegível)
        is_blocked = p.get('is_technically_blocked') or p.get('is_protected_area_overlap')
        cor_laudo = (192, 57, 43) if is_blocked else (39, 174, 96)
        status_final = "DESFAVORÁVEL" if is_blocked else "FAVORÁVEL"

        pdf = CaiporaReport(status_cor=cor_laudo)
        pdf.add_page()
        
        # 1. CABEÇALHO: IDENTIFICAÇÃO
        pdf.section_title("1. Cabeçalho: Identificação da Propriedade")
        pdf.field("Nome da Propriedade", p['property_alias'])
        pdf.field("Código CAR Oficial", p['property_id'])
        pdf.field("Município / UF", f"{p['city']} / {p['uf_origem']}")
        pdf.field("Bioma", p.get('bioma_name', 'Amazônia'))
        pdf.field("Área Total", f"{p.get('area_ha', 0):.2f} hectares")
        pdf.field("Coordenadas Geográficas", f"Lat {p.get('latitude')} / Long {p.get('longitude')}")
        pdf.field("Data de Processamento", str(p.get('processed_at')))
        pdf.field("Data da Análise Pericial", datetime.now().strftime('%d/%m/%Y às %H:%M:%S'))
        pdf.ln(5)

        # 2. METODOLOGIA TÉCNICA
        pdf.section_title("2. Metodologia Técnica")
        pdf.set_font('helvetica', '', 9)
        metodologia = (
            "A presente análise de conformidade foi realizada mediante o rigoroso cruzamento de dados geoespaciais "
            "multitemporais, utilizando as seguintes bases de inteligência: SICAR (Reserva Legal/APP), MapBiomas (Desmatamento), "
            "IBAMA (Embargos), Ministério do Trabalho (Lista Suja) e EUDR Compliance (Rastreabilidade Europeia)."
        )
        pdf.multi_cell(0, 5, metodologia, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(5)

        # 3. DOSSIÊ DE PASSIVOS E INFRAÇÕES
        pdf.section_title("3. Dossiê de Passivos e Infrações")
        
        # Alerta de Reserva Legal
        rl_deficit = p.get('rl_deficit_ha', 0)
        pdf.field("Déficit de Reserva Legal", f"{rl_deficit:.2f} ha", alert=rl_deficit > 0)
        
        # Alerta de Unidade de Conservação
        is_uc = p.get('is_protected_area_overlap')
        uc_texto = f"CONFLITO IDENTIFICADO ({p.get('protected_area_overlap_ha', 0):.2f} ha)" if is_uc else "ZERO SOBREPOSIÇÃO"
        pdf.field("Unidades de Conservação", uc_texto, alert=is_uc)
        
        # Alerta de Embargos
        is_embargo = p.get('is_embargo_active')
        embargo_texto = "BLOQUEIO ATIVO / EMBARGADO" if is_embargo else "SEM BLOQUEIOS ATIVOS"
        pdf.field("Embargos e Autuações", embargo_texto, alert=is_embargo)
        pdf.field("Processos", p.get('embargo_processes', '0 ocorrências'))
        pdf.ln(5)

        # 4. ANÁLISE DE RISCO FINANCEIRO E MERCADO
        pdf.section_title("4. Análise de Risco Financeiro e Mercado")
        passivo_brl = p.get('estimated_financial_liability_brl', 0)
        pdf.field("Passivo Financeiro Estimado", f"R$ {passivo_brl:,.2f}", alert=passivo_brl > 0)
        
        is_eudr = p.get('is_eudr_restricted')
        pdf.field("Risco de Exportação (EUDR)", "RESTRITO" if is_eudr else "ELEGÍVEL", alert=is_eudr)
        pdf.field("Segurança Jurídica", p.get('geospatial_confidence_level', 'N/A'))
        
        pdf.set_font('helvetica', 'I', 8)
        pdf.multi_cell(0, 4, "Nota: O status HIGH_CONFIDENCE indica validação espacial completa. Status LOW_CONFIDENCE sinalizaria insegurança jurídica na garantia imobiliária.", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(5)

        # 5. CONCLUSÃO E ENQUADRAMENTO LEGAL
        pdf.section_title(f"5. Conclusão: PARECER {status_final}")
        pdf.set_font('helvetica', 'B' if is_blocked else '', 9)
        
        if is_blocked:
            conclusao = (
                f"Diante do exposto, emito PARECER DESFAVORÁVEL. A propriedade apresenta impedimentos críticos, "
                f"com destaque para a sobreposição de {p.get('protected_area_overlap_ha', 0):.2f} ha com áreas protegidas. "
                f"Tais condições violam a Resolução CMN nº 5.081/2023, tornando o ativo INAPTO para crédito rural."
            )
        else:
            conclusao = (
                f"Diante do exposto, emito PARECER FAVORÁVEL. A propriedade encontra-se plenamente enquadrada "
                f"na Lei 12.651/2012 e atende aos critérios da Resolução CMN nº 5.081/2023. Ativo de Baixo Risco."
            )
        
        pdf.multi_cell(0, 5, conclusao, new_x="LMARGIN", new_y="NEXT")

        # --- PÁGINA 2: EVIDÊNCIA CARTOGRÁFICA ---
        map_img = generate_map(p['geometry'])
        if map_img:
            pdf.add_page()
            pdf.section_title("Evidência Cartográfica e Geometria")
            pdf.image(map_img, x=10, y=50, w=190)
            pdf.set_y(-40)
            pdf.set_font('helvetica', 'I', 8)
            pdf.cell(0, 5, "Figura 1: Polígono do imóvel sobreposto à imagem de satélite de alta resolução.", align='C')

        # --- SALVAMENTO E UPLOAD ---
        file_name = f"LAUDO_{p['property_id'].replace('-', '_')}.pdf"
        local_path = os.path.join("laudos_finais", file_name)
        pdf.output(local_path)
        
        # Upload para o Google Cloud Storage
        blob_path = f"reports/periciais/{p['uf_origem']}/{p['city']}/{file_name}"
        bucket.blob(blob_path).upload_from_filename(local_path)
        
        logging.info(f"✅ Laudo {status_final} gerado e enviado: {blob_path}")

if __name__ == "__main__":
    run_report()
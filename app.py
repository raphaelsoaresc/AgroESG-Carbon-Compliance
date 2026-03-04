import streamlit as st
import pandas as pd
import duckdb
import os

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO VISUAL
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Caipora Sentinela | Auditoria ESG", page_icon="📋", layout="wide")

st.markdown("""
    <style>
    header[data-testid="stHeader"], footer {display: none;}
    .stApp {background-color: #0f1915;}
    div[data-testid="metric-container"] {
        background-color: #14211d; border: 1px solid #2d4a3e; padding: 15px; border-radius: 4px;
    }
    label[data-testid="stMetricLabel"] {color: #10b981 !important; font-weight: bold;}
    div[data-testid="stMetricValue"] {color: #ffffff !important;}
    div[data-testid="stDataFrame"] {border: 1px solid #2d4a3e;}
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. CARREGAMENTO E TRATAMENTO
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    parquet_file = "data_compliance.parquet" 
    if not os.path.exists(parquet_file): return pd.DataFrame()
    
    con = duckdb.connect(database=':memory:')
    df = con.execute(f"SELECT * FROM '{parquet_file}'").df()
    
    # 1. Máscara LGPD para Trabalho Escravo
    def mask_slave_labor(val):
        if pd.isna(val) or str(val).strip() in ["", "None", "nan"]: return "✅ Regular"
        return "⚠️ REGISTRO IDENTIFICADO"
    
    if 'slave_labor_offender' in df.columns:
        df['slave_labor_offender_masked'] = df['slave_labor_offender'].apply(mask_slave_labor)
    
    # 2. Tratamento de Datas (Esconder a data dummy 1900-01-01 do dbt)
    if 'embargo_date' in df.columns:
        df['embargo_date'] = pd.to_datetime(df['embargo_date'])
        df['embargo_date_str'] = df['embargo_date'].dt.strftime('%d/%m/%Y')
        df.loc[df['embargo_date'].dt.year <= 1900, 'embargo_date_str'] = "S/ Data"

    # 3. Coordenadas formatadas
    if 'latitude' in df.columns and 'longitude' in df.columns:
        df['coordinates'] = df.apply(
            lambda x: f"{x['latitude']:.5f}, {x['longitude']:.5f}" if pd.notnull(x['latitude']) else "N/A", axis=1
        )
    
    # 4. Limpeza de NaNs
    numeric_cols = ['max_slope_degrees', 'app_ndvi_mean', 'general_ndvi_mean', 'embargo_area_ha', 'property_area_ha']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)
            
    return df

df = load_data()

# -----------------------------------------------------------------------------
# 3. CABEÇALHO E FILTROS
# -----------------------------------------------------------------------------
st.markdown("## CAIPORA <span style='color:#10b981'>SENTINELA</span> | AUDITORIA ESG", unsafe_allow_html=True)

if df.empty:
    st.error("Arquivo de dados não encontrado.")
    st.stop()

with st.container():
    f1, f2, f3 = st.columns([1, 1.5, 2])
    with f1: sel_biome = st.multiselect("Bioma", sorted(df['biome_name'].unique()))
    with f2: sel_status = st.multiselect("Status de Elegibilidade", sorted(df['final_eligibility_status'].unique()))
    with f3: search = st.text_input("🔍 Buscar por Código CAR ou Coordenadas", placeholder="Ex: MT-51...")

df_show = df.copy()
if sel_biome: df_show = df_show[df_show['biome_name'].isin(sel_biome)]
if sel_status: df_show = df_show[df_show['final_eligibility_status'].isin(sel_status)]
if search:
    df_show = df_show[
        df_show['property_id'].str.contains(search, case=False) | 
        df_show['coordinates'].str.contains(search)
    ]

# -----------------------------------------------------------------------------
# 4. KPIs (Ajustados para os novos status)
# -----------------------------------------------------------------------------
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Imóveis", f"{len(df_show):,}".replace(",", "."))
k2.metric("Área Total (ha)", f"{df_show['property_area_ha'].sum():,.0f}".replace(",", "."))
k3.metric("🚨 Bloqueios", len(df_show[df_show['final_eligibility_status'].str.contains('NOT ELIGIBLE')]))
k4.metric("🚧 Adjacência", len(df_show[df_show['final_eligibility_status'].str.contains('ADJACENCY')]))
k5.metric("☁️ Pendentes", len(df_show[df_show['final_eligibility_status'].str.contains('CLEARANCE')]))

# -----------------------------------------------------------------------------
# 5. TABELA DINÂMICA
# -----------------------------------------------------------------------------

# Colunas essenciais
cols_to_display = ['property_id', 'final_eligibility_status', 'internal_risks_found', 'technical_evidence']

# Lógica de colunas dinâmicas baseada nas TAGS do dbt
all_tags = " ".join(df_show['internal_risks_found'].fillna("").unique()).upper()

if "SOCIAL" in all_tags:
    cols_to_display.append('slave_labor_offender_masked')
if "SATELLITE" in all_tags or "SLOPE" in all_tags:
    cols_to_display.extend(['max_slope_degrees', 'app_ndvi_mean'])
if "IBAMA" in all_tags or "EMBARGO" in all_tags:
    cols_to_display.extend(['embargo_area_ha', 'embargo_date_str'])
if "ADJACENCY" in " ".join(df_show['final_eligibility_status'].unique()).upper():
    cols_to_display.append('adjacency_details')

cols_to_display.extend(['coordinates', 'property_alias'])
cols_to_display = list(dict.fromkeys(cols_to_display)) # Remove duplicatas

column_config = {
    "property_id": st.column_config.TextColumn("Código CAR"),
    "final_eligibility_status": st.column_config.TextColumn("Status Final"),
    "internal_risks_found": st.column_config.TextColumn("🏷️ Tags de Risco"),
    "technical_evidence": st.column_config.TextColumn("⚖️ Evidência Técnica", width="large"),
    "slave_labor_offender_masked": st.column_config.TextColumn("Lista Suja"),
    "max_slope_degrees": st.column_config.NumberColumn("Declividade", format="%.1f°"),
    "app_ndvi_mean": st.column_config.NumberColumn("NDVI APP", format="%.3f"),
    "embargo_area_ha": st.column_config.NumberColumn("Embargo (ha)", format="%.2f"),
    "embargo_date_str": st.column_config.TextColumn("Data Embargo"),
    "coordinates": st.column_config.TextColumn("📍 Localização"),
    "property_alias": st.column_config.TextColumn("ID Interno"),
}

st.dataframe(
    df_show[cols_to_display].sort_values(by='final_eligibility_status'),
    column_config=column_config,
    use_container_width=True,
    height=600,
    hide_index=True
)
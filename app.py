import streamlit as st
import pandas as pd
import duckdb
import os

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO VISUAL (TEMA AUDITORIA)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Caipora Sentinela | Auditoria",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
    header[data-testid="stHeader"] {display: none;}
    footer {display: none;}
    .block-container {padding-top: 2rem; padding-bottom: 2rem;}
    .stApp {background-color: #0f1915;}
    h1, h2, h3, h4, p, div {font-family: 'Segoe UI', sans-serif;}
    
    /* KPIs */
    div[data-testid="metric-container"] {
        background-color: #1e332a;
        border: 1px solid #2d4a3e;
        padding: 15px;
        border-radius: 8px;
    }
    label[data-testid="stMetricLabel"] {color: #a7f3d0 !important;}
    div[data-testid="stMetricValue"] {color: #e2e8f0 !important;}
    
    /* Tabela */
    div[data-testid="stDataFrame"] {
        border: 1px solid #2d4a3e; 
        background-color: #14211d;
    }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. CARREGAMENTO DE DADOS (SCHEMA ATUALIZADO)
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    parquet_file = "/home/obscuritenoir/Portfolio/AgroESG-Carbon-Compliance/data_compliance.parquet"
    
    if not os.path.exists(parquet_file):
        return pd.DataFrame()
    
    con = duckdb.connect(database=':memory:')
    
    # Seleciona as colunas geradas pelo DBT atualizado
    query = f"""
        SELECT 
            property_id,
            property_alias,
            biome_name, 
            final_eligibility_status, 
            property_area_ha,
            latitude, longitude,
            
            -- Evidências Sociais
            slave_labor_offender,
            slave_labor_overlap_ha,
            
            -- Evidências Ambientais (Embargo)
            embargo_date,
            embargo_area_ha,
            
            -- Evidências Ambientais (Protegidas)
            protected_overlap_ha,
            indigenous_overlap_ha,
            quilombola_overlap_ha,
            uc_overlap_ha,
            invaded_territory_names,
            app_overlap_ha,
            is_app_deforested, -- <-- Nova coluna de prova do crime em APP
            
            -- Evidências Adjacência
            adjacent_risk_types,
            adjacency_border_km,
            
            -- Outros / Satélite
            rl_status,
            rl_balance_ha,
            vegetation_state,
            ndvi_mean,
            max_slope_degrees,
            is_satellite_violation
            
        FROM '{parquet_file}'
    """
    df = con.execute(query).df()
    
    # --- TRATAMENTOS ---
    
    df['property_alias_masked'] = df['property_id'].apply(lambda x: f"FAZENDA PROTEGIDA ****{str(x)[-4:]}")
    
    def mask_lgpd(val):
        if pd.isna(val) or str(val).strip() == "": return "Regular"
        return "⚠️ REGISTRO IDENTIFICADO (DADOS OCULTOS)"
    df['slave_labor_offender'] = df['slave_labor_offender'].apply(mask_lgpd)
    
    df['coordinates'] = df.apply(
        lambda x: f"{x['latitude']:.5f}, {x['longitude']:.5f}" 
        if pd.notnull(x['latitude']) and pd.notnull(x['longitude']) else "N/A", axis=1
    )
    
    cols_to_zero = [
        'embargo_area_ha', 'protected_overlap_ha', 'rl_balance_ha', 
        'slave_labor_overlap_ha', 'indigenous_overlap_ha', 
        'quilombola_overlap_ha', 'uc_overlap_ha', 'adjacency_border_km',
        'app_overlap_ha'
    ]
    for col in cols_to_zero:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    
    # Trata a nova flag como booleana
    if 'is_app_deforested' in df.columns:
        df['is_app_deforested'] = df['is_app_deforested'].fillna(False).astype(bool)
            
    df['invaded_territory_names'] = df['invaded_territory_names'].fillna("-")
    df['adjacent_risk_types'] = df['adjacent_risk_types'].fillna("-")
            
    return df

df = load_data()

if df.empty:
    st.error("❌ Arquivo de dados não encontrado. Verifique se o dbt rodou e gerou o arquivo parquet.")
    st.stop()

# -----------------------------------------------------------------------------
# 3. CABEÇALHO
# -----------------------------------------------------------------------------
col_logo, col_title = st.columns([1, 8])
with col_logo:
    st.markdown("""<div style="background-color: #1e332a; width: 70px; height: 70px; border-radius: 10px; display: flex; align-items: center; justify-content: center; border: 2px solid #d97706;"><span style="font-size: 35px;">🐗</span></div>""", unsafe_allow_html=True)
with col_title:
    st.markdown("## CAIPORA <span style='color:#d97706'>SENTINELA</span>", unsafe_allow_html=True)
    st.markdown("<p style='color:#94a3b8; margin-top: -15px;'>Auditoria de Compliance Geoespacial</p>", unsafe_allow_html=True)

st.markdown("---")

# -----------------------------------------------------------------------------
# 4. FILTROS
# -----------------------------------------------------------------------------
st.markdown("### 🔍 Filtros")
f1, f2, f3 = st.columns([1, 2, 1])

with f1:
    sel_biome = st.multiselect("Bioma", sorted(df['biome_name'].dropna().unique()), default=[])
with f2:
    status_opts = sorted(df['final_eligibility_status'].dropna().unique())
    sel_status = st.multiselect("Status de Risco", status_opts, default=[])
with f3:
    search = st.text_input("Buscar ID", placeholder="PROP-...")

df_show = df.copy()
if sel_biome:
    df_show = df_show[df_show['biome_name'].isin(sel_biome)]
if sel_status:
    df_show = df_show[df_show['final_eligibility_status'].isin(sel_status)]
if search:
    df_show = df_show[df_show['property_id'].astype(str).str.contains(search, case=False)]

# -----------------------------------------------------------------------------
# 4.5. KPIs
# -----------------------------------------------------------------------------
st.markdown("### 📊 Visão Geral")
k1, k2, k3, k4 = st.columns(4)

total_props = len(df_show)
total_area = df_show['property_area_ha'].sum()
elegiveis = len(df_show[df_show['final_eligibility_status'] == 'ELIGIBLE'])
bloqueadas = len(df_show[df_show['final_eligibility_status'].str.contains('NOT ELIGIBLE', na=False)])

with k1:
    st.metric("Total de Propriedades", f"{total_props:,}".replace(",", "."))
with k2:
    st.metric("Área Total (ha)", f"{total_area:,.0f}".replace(",", "."))
with k3:
    st.metric("✅ Elegíveis", f"{elegiveis:,}".replace(",", "."))
with k4:
    st.metric("🚨 Críticas (Bloqueadas)", f"{bloqueadas:,}".replace(",", "."))

st.markdown("---")

# -----------------------------------------------------------------------------
# 5. LÓGICA DE COLUNAS DINÂMICAS
# -----------------------------------------------------------------------------
cols_to_display = ['property_alias_masked', 'final_eligibility_status', 'coordinates', 'biome_name', 'property_area_ha']
present_statuses = " ".join(df_show['final_eligibility_status'].dropna().unique()).upper()

if "SLAVE LABOR" in present_statuses:
    cols_to_display.extend(["slave_labor_offender", "slave_labor_overlap_ha"])

if "EMBARGO" in present_statuses:
    cols_to_display.extend(["embargo_area_ha", "embargo_date"])

if "PROTECTED AREA" in present_statuses:
    cols_to_display.extend(["protected_overlap_ha", "invaded_territory_names", "indigenous_overlap_ha", "uc_overlap_ha", "quilombola_overlap_ha"])

if "ADJACENCY" in present_statuses:
    cols_to_display.extend(["adjacent_risk_types", "adjacency_border_km"])

if "DEFICIT" in present_statuses:
    cols_to_display.extend(["rl_status", "rl_balance_ha"])

if "CLOUDS" in present_statuses:
    cols_to_display.extend(["vegetation_state"])

if "SATELLITE ALERT" in present_statuses or "DEFORESTATION" in present_statuses:
    cols_to_display.extend(["is_satellite_violation", "ndvi_mean"])

# Ajuste para os novos status de INFO e Bloqueio Justo de APP
if "APP INVASION" in present_statuses or "APP AREA" in present_statuses:
    if "HIGH SLOPE" in present_statuses:
        cols_to_display.append("max_slope_degrees")
    if "WATER BODIES" in present_statuses:
        # Adiciona a área do rio e a nova flag de desmatamento
        cols_to_display.extend(["app_overlap_ha", "is_app_deforested"])

cols_to_display = list(dict.fromkeys(cols_to_display))

# -----------------------------------------------------------------------------
# 6. TABELA
# -----------------------------------------------------------------------------
st.markdown(f"### 📋 Detalhamento ({len(df_show)} registros filtrados)")

column_config = {
    "property_alias_masked": st.column_config.TextColumn("Propriedade", width="medium"),
    "final_eligibility_status": st.column_config.TextColumn("Status", width="large"),
    "property_area_ha": st.column_config.ProgressColumn("Área Total", format="%d ha", min_value=0, max_value=int(df['property_area_ha'].max()) if not df.empty else 100),
    "coordinates": st.column_config.TextColumn("Lat, Lon", width="medium"),
    
    "slave_labor_offender": st.column_config.TextColumn("⚠️ Infrator (Lista Suja)", width="medium"),
    "slave_labor_overlap_ha": st.column_config.NumberColumn("Área Risco Social", format="%.2f ha"),
    
    "embargo_area_ha": st.column_config.NumberColumn("⛔ Área Embargada", format="%.2f ha"),
    "embargo_date": st.column_config.DateColumn("Data Embargo"),
    
    "protected_overlap_ha": st.column_config.NumberColumn("🌲 Área Invadida Total", format="%.2f ha"),
    "invaded_territory_names": st.column_config.TextColumn("Nome do Território", width="large"),
    "indigenous_overlap_ha": st.column_config.NumberColumn("Sobreposição TI", format="%.2f ha"),
    "uc_overlap_ha": st.column_config.NumberColumn("Sobreposição UC", format="%.2f ha"),
    "quilombola_overlap_ha": st.column_config.NumberColumn("Sobreposição Quilombo", format="%.2f ha"),
    
    # Configuração da nova prova do crime em APP
    "app_overlap_ha": st.column_config.NumberColumn("💧 Área APP Hídrica", format="%.2f ha"),
    "is_app_deforested": st.column_config.CheckboxColumn("🪓 Desmatamento em APP"),
    
    "is_satellite_violation": st.column_config.CheckboxColumn("🚨 Alerta Desmatamento Geral"),
    "ndvi_mean": st.column_config.NumberColumn("Índice NDVI", format="%.3f"),
    
    "adjacent_risk_types": st.column_config.TextColumn("Risco do Vizinho", width="medium"),
    "adjacency_border_km": st.column_config.NumberColumn("🚧 Fronteira de Contato", format="%.2f km"),
    
    "rl_status": st.column_config.TextColumn("Status CAR"),
    "rl_balance_ha": st.column_config.NumberColumn("⚖️ Saldo RL", format="%.2f ha"),
    "vegetation_state": st.column_config.TextColumn("☁️ Nuvens"),
    "max_slope_degrees": st.column_config.NumberColumn("Declive Máx", format="%.1f°"),
}

df_show = df_show.sort_values(by=['final_eligibility_status'], ascending=False)

st.dataframe(
    df_show[cols_to_display], 
    column_config=column_config,
    use_container_width=True,
    height=600,
    hide_index=True
)
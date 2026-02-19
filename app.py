import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk
import json
import gc

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="AgroMarte Sentinela | MT",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. CARGA DE DADOS OTIMIZADA ---
@st.cache_data(ttl=3600)
def load_data():
    # Carrega apenas colunas necessárias para economizar RAM inicial
    cols = ['property_alias', 'biome_name', 'eligibility_status', 'property_area_ha', 
            'embargo_overlap_ha', 'embargo_date', 'latitude', 'longitude', 'geom_json']
    
    df = pd.read_parquet('data_compliance.parquet', columns=cols, engine='pyarrow')
    
    # Otimização de tipos (RAM)
    numeric_cols = ['property_area_ha', 'embargo_overlap_ha', 'latitude', 'longitude']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float32')
    
    # Categorização para performance de filtro
    df['biome_name'] = df['biome_name'].astype('category')
    df['eligibility_status'] = df['eligibility_status'].astype('category')

    if 'latitude' in df.columns and 'longitude' in df.columns:
        df['coords'] = df['latitude'].round(4).astype(str) + ", " + df['longitude'].round(4).astype(str)

    return df

with st.spinner('🛡️ AgroMarte Sentinela: Acessando Inteligência Territorial...'):
    df = load_data()

# --- 3. BARRA LATERAL (FILTROS) ---
st.sidebar.title("🛡️ Sentinela MT")

biomes_list = sorted(df['biome_name'].unique().tolist())
default_biomes = [b for b in biomes_list if "AMAZ" in str(b).upper()]
selected_biomes = st.sidebar.multiselect("Biomas", options=biomes_list, default=default_biomes)

status_list = sorted(df['eligibility_status'].unique().tolist())
default_status = [s for s in status_list if "ELIGIBLE" in str(s) and "NOT" not in str(s)]
selected_status = st.sidebar.multiselect("Status de Compliance", options=status_list, default=default_status)

df_filtered = df[
    (df['biome_name'].isin(selected_biomes)) & 
    (df['eligibility_status'].isin(selected_status))
].copy()

# --- 4. KPIs ---
st.title("🛡️ AgroMarte Sentinela")
st.markdown(f"**Mato Grosso (MT)** | Monitoramento Geoespacial | **{len(df_filtered):,}** propriedades")

k1, k2, k3, k4 = st.columns(4)

# MÁSCARAS DE STATUS (Crucial para o seu Backend)
mask_eligible = df_filtered['eligibility_status'].astype(str).str.upper().str.contains('ELIGIBLE', na=False) & \
                ~df_filtered['eligibility_status'].astype(str).str.upper().str.contains('NOT', na=False)

mask_not_eligible = df_filtered['eligibility_status'].astype(str).str.upper().str.contains('NOT ELIGIBLE', na=False)

# CÁLCULO ÁREA ÚTIL (Apenas do que é comprável)
df_only_eligible = df_filtered[mask_eligible]
area_total_eligible = df_only_eligible['property_area_ha'].sum()
overlap_eligible = df_only_eligible['embargo_overlap_ha'].sum()
area_util = area_total_eligible - overlap_eligible

# CÁLCULO BLOQUEIO AMAZÔNIA
mask_amazon = df_filtered['eligibility_status'].astype(str).str.upper().str.contains('AMAZON', na=False) & mask_not_eligible
area_bloqueio = df_filtered[mask_amazon]['property_area_ha'].sum()

# KPI DE SEVERIDADE (FOCO EM TOMADA DE DECISÃO)
# Calculamos a severidade APENAS sobre as propriedades que estão bloqueadas.
# Isso mostra o quão 'sujas' elas estão de fato.
df_ne = df_filtered[mask_not_eligible]
total_ne = df_ne['property_area_ha'].sum()
overlap_ne = df_ne['embargo_overlap_ha'].sum()
severity = (overlap_ne / total_ne * 100) if total_ne > 0 else 0

# NETWORK RISK (CONTAMINAÇÃO)
mask_contam = df_filtered['eligibility_status'].astype(str).str.upper().str.contains('CONTAMINATION', na=False)
net_risk = mask_contam.sum()

# EXIBIÇÃO
k1.metric("🌱 Área Útil (MT)", f"{area_util:,.0f} ha")
k2.metric("🌳 Bloqueio Amazônia", f"{area_bloqueio:,.0f} ha")
k3.metric("⚠️ Severidade (Bloqueados)", f"{severity:.2f}%") # Nome alterado para clareza
k4.metric("☣️ Network Risk", f"{net_risk}")

# --- 5. VISUALIZAÇÃO ---
st.subheader("🗺️ Inteligência Territorial - Mato Grosso")

st.markdown("""
    <div style="display: flex; gap: 15px; margin-bottom: 10px; font-size: 13px; color: #CCC;">
        <span><b style="color: #8B0000;">●</b> Crítico Amazônia</span>
        <span><b style="color: #FF0000;">●</b> Não Elegível</span>
        <span><b style="color: #800080;">●</b> Contaminação</span>
        <span><b style="color: #FFD700;">●</b> Consolidado</span>
        <span><b style="color: #008000;">●</b> Elegível</span>
    </div>
    """, unsafe_allow_html=True)

col_map, col_chart = st.columns([2, 1])

def get_color_rgb(status):
    s = str(status).upper()
    if 'CRITICAL AMAZON' in s: return [139, 0, 0, 200]
    if 'CONTAMINATION' in s: return [128, 0, 128, 200]
    if 'NOT ELIGIBLE' in s: return [255, 0, 0, 200]
    if 'CONSOLIDATED' in s: return [255, 215, 0, 200]
    if 'ELIGIBLE' in s: return [0, 128, 0, 200]
    return [150, 150, 150, 150]

with col_map:
    total_visivel = len(df_filtered)
    view_state = pdk.ViewState(latitude=-12.6, longitude=-55.7, zoom=5.5)
    
    # Fix: astype(str) antes do apply resolve o erro de 'unhashable type: list'
    if total_visivel > 1000: # Threshold reduzido para segurança mobile
        st.caption(f"Exibindo {total_visivel:,} pontos (Performance). Filtre para ver polígonos.")
        df_filtered['color'] = df_filtered['eligibility_status'].astype(str).apply(get_color_rgb)
        layer = pdk.Layer(
            "ScatterplotLayer", df_filtered,
            get_position=["longitude", "latitude"],
            get_fill_color="color", get_radius=1000, pickable=True,
        )
    else:
        st.caption(f"Exibindo {total_visivel} polígonos detalhados.")
        df_map = df_filtered.copy()
        df_map['geometry'] = df_map['geom_json'].apply(json.loads)
        df_map['color'] = df_map['eligibility_status'].astype(str).apply(get_color_rgb)
        layer = pdk.Layer(
            "GeoJsonLayer", df_map,
            get_polygon="geometry", get_fill_color="color",
            get_line_color=[255, 255, 255, 60], get_line_width=10,
            pickable=True, auto_highlight=True,
        )

    st.pydeck_chart(pdk.Deck(
        layers=[layer], 
        initial_view_state=view_state,
        map_style="dark",  # Use apenas a palavra 'light' para fundo branco com cidades
        tooltip={"html": "<b>Alias:</b> {property_alias} <br/> <b>Status:</b> {eligibility_status}"}
    ))
with col_chart:
    st.subheader("📊 Risco por Bioma")
    if not df_filtered.empty:
        df_risk = df_filtered.groupby(['biome_name', 'eligibility_status'], observed=True)['property_area_ha'].sum().reset_index()
        color_map = {
            'NOT ELIGIBLE - CRITICAL AMAZON VIOLATION': '#8B0000',
            'NOT ELIGIBLE - RISK BY OVERLAP (CONTAMINATION)': '#800080',
            'NOT ELIGIBLE - POST-2008 VIOLATION': '#FF0000',
            'ELIGIBLE W/ MONITORING (CONSOLIDATED)': '#FFD700',
            'ELIGIBLE': '#008000',
            'ELIGIBLE - NEGLIGIBLE OVERLAP': '#90EE90'
        }
        fig = px.bar(df_risk, x='property_area_ha', y='biome_name', color='eligibility_status',
                    orientation='h', color_discrete_map=color_map, template="plotly_dark")
        fig.update_layout(showlegend=False, height=400, margin=dict(t=0,b=0,l=0,r=0))
        st.plotly_chart(fig, use_container_width=True)

# --- 6. TABELA ---
st.markdown("---")
st.subheader("📋 Detalhamento Técnico")
cols = ['property_alias', 'biome_name', 'eligibility_status', 'property_area_ha', 'embargo_date', 'coords']
st.dataframe(df_filtered[cols].head(100), use_container_width=True, hide_index=True)

gc.collect()

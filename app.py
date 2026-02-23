import streamlit as st
import pandas as pd
import pydeck as pdk
import json
import os

st.set_page_config(page_title="AgroESG Compliance", layout="wide")

# --- CARREGAMENTO DE DADOS (BLINDADO CONTRA DUPLICATAS) ---
@st.cache_data
def load_data():
    path = '/home/obscuritenoir/Portfolio/AgroESG-Carbon-Compliance/data_compliance.parquet'
    if not os.path.exists(path):
        st.error("Arquivo Parquet não encontrado.")
        st.stop()
        
    df = pd.read_parquet(path)
    
    # 1. Força a remoção de qualquer coluna duplicada por nome
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 2. Se ainda assim 'eligibility_status' for um DataFrame (duplicata oculta), pega a primeira
    if isinstance(df.get('final_eligibility_status'), pd.DataFrame):
        df['final_eligibility_status'] = df['final_eligibility_status'].iloc[:, 0]
    
    if 'final_eligibility_status' in df.columns:
        df['eligibility_status'] = df['final_eligibility_status']
    
    # 3. Garante que eligibility_status seja uma Series única antes de usar .str
    status_series = df['eligibility_status']
    if isinstance(status_series, pd.DataFrame):
        status_series = status_series.iloc[:, 0]
        
    df['eligibility_status'] = status_series.astype(str).str.strip().astype('category')
    df['biome_name'] = df['biome_name'].astype(str).str.strip().astype('category')
    
    return df

df = load_data()

# --- SIDEBAR: MODOS DE ANÁLISE (CLUSTERS) ---
st.sidebar.title("🧐 Modo de Auditoria")

# O Cluster selecionado aqui vai CONFIGURAR os filtros automaticamente
cluster_choice = st.sidebar.radio(
    "Selecione o Cluster de Análise:",
    [
        "🛡️ Compliance Total",
        "🚫 Bloqueios Críticos",
        "💰 Originação de Crédito",
        "🛰️ Monitoramento de Risco"
    ]
)

st.sidebar.divider()

# --- LÓGICA DE PRESETS POR CLUSTER ---
all_statuses = df['eligibility_status'].unique().tolist()

if cluster_choice == "🛡️ Compliance Total":
    st.sidebar.info("Exibindo visão holística de todas as propriedades.")
    preset_status = all_statuses
    preset_app = False
    preset_ti = False

elif cluster_choice == "🚫 Bloqueios Críticos":
    st.sidebar.warning("Foco em Terras Indígenas, Quilombolas e APPs.")
    preset_status = [s for s in all_statuses if "OVERLAP" in s or "CRITICAL APP" in s]
    preset_app = True
    preset_ti = True

elif cluster_choice == "💰 Originação de Crédito":
    st.sidebar.success("Foco em propriedades aptas para operação.")
    preset_status = [s for s in all_statuses if s.startswith("ELIGIBLE")]
    preset_app = False
    preset_ti = False

elif cluster_choice == "🛰️ Monitoramento de Risco":
    st.sidebar.error("Foco em desmatamento na Amazônia e Adjacência.")
    preset_status = [s for s in all_statuses if "AMAZON" in s or "ADJACENCY" in s]
    preset_app = False
    preset_ti = False

# --- EXPANDER PARA AJUSTE FINO (OPCIONAL) ---
with st.sidebar.expander("⚙️ Ajuste Fino (Filtros do Cluster)"):
    selected_status = st.multiselect("Status", options=all_statuses, default=preset_status)
    only_app = st.checkbox("Filtrar APPs", value=preset_app)
    only_ti = st.checkbox("Filtrar Terras Indígenas", value=preset_ti)

# --- FILTRAGEM ---
filtered_df = df[df['eligibility_status'].isin(selected_status)].copy()
if only_app: filtered_df = filtered_df[filtered_df['has_app_area'] == True]
if only_ti: filtered_df = filtered_df[filtered_df['is_indigenous_land'] == True]

# --- DASHBOARD ---
st.title(f"📊 Painel: {cluster_choice}")

# KPIs
c1, c2, c3, c4 = st.columns(4)
if not filtered_df.empty:
    c1.metric("Propriedades", f"{len(filtered_df):,}")
    eligible_count = len(filtered_df[filtered_df['eligibility_status'].astype(str).str.startswith('ELIGIBLE')])
    c2.metric("% Elegível", f"{(eligible_count/len(filtered_df)*100):.1f}%")
    c3.metric("Área Total (ha)", f"{filtered_df['property_area_ha'].sum():,.0f}")
    c4.metric("Alertas APP", f"{int(filtered_df['has_app_area'].sum())}")

    # --- MAPA ---
    color_map = {
        "ELIGIBLE": [46, 204, 113, 150],
        "NOT ELIGIBLE - INDIGENOUS LAND OVERLAP": [155, 89, 182, 200],
        "NOT ELIGIBLE - CRITICAL APP VIOLATION": [52, 152, 219, 200],
        "NOT ELIGIBLE - CRITICAL AMAZON VIOLATION": [231, 76, 60, 200],
        "NOT ELIGIBLE - RISK BY ADJACENCY (CONTAMINATION)": [241, 196, 15, 200],
        "OTHER": [149, 165, 166, 100]
    }

    def assign_color(status):
        return color_map.get(str(status).strip(), color_map["OTHER"])

    # List comprehension para evitar erro de unhashable list
    filtered_df['color'] = [assign_color(s) for s in filtered_df['eligibility_status']]
    
    view_mode = st.radio("Nível de Detalhe:", ["Pontos", "Polígonos"], horizontal=True)
    
    layers = []
    if view_mode == "Pontos":
        layers.append(pdk.Layer("ScatterplotLayer", filtered_df, get_position=["longitude", "latitude"], get_color="color", get_radius=600, pickable=True))
    else:
        filtered_df['geojson'] = [json.loads(g) if g else None for g in filtered_df['geom_json']]
        layers.append(pdk.Layer("GeoJsonLayer", filtered_df, get_polygon="geojson", get_fill_color="color", get_line_color=[255, 255, 255], line_width_min_pixels=1, pickable=True))

    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=-12.6, longitude=-55.4, zoom=6),
        tooltip={"text": "{property_alias}\nStatus: {eligibility_status}"}
    ))
else:
    st.warning("Nenhum dado encontrado para os critérios deste cluster.")
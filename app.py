import streamlit as st
import pandas as pd
import duckdb
import os
import re

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO E ESTILO
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Caipora Sentinela | Auditoria MapBiomas",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_css(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Tenta carregar CSS se existir
try:
    load_css("assets/style.css")
except:
    pass

st.markdown("""
    <div class="custom-header">
        <div style="display: flex; align-items: center; gap: 15px;">
            <img src="https://brasil.mapbiomas.org/wp-content/uploads/sites/4/2024/10/Logo-MapBiomas-Geral.png" height="50" alt="MapBiomas">
            <div style="border-left: 2px solid #E5E7EB; height: 40px;"></div>
            <div class="header-title">
                <h1>Caipora <span class="text-mapbiomas">Sentinela</span></h1>
                <div class="header-subtitle">Auditoria de Conformidade Ambiental</div>
            </div>
        </div>
        <div style="text-align: right;">
            <span style="font-weight: 600; color: #4B5563;">Prêmio MapBiomas 2026</span><br>
            <span style="font-size: 0.8rem; color: #9CA3AF;">Categoria: Negócios</span>
        </div>
    </div>
    <hr>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. CARREGAMENTO DE DADOS E LÓGICA DE NEGÓCIO
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    if not os.path.exists("data_compliance.parquet"):
        st.error("Arquivo de dados 'data_compliance.parquet' não encontrado.")
        return pd.DataFrame()

    con = duckdb.connect(database=':memory:')
    df = con.execute(f"SELECT * FROM 'data_compliance.parquet'").df()
    
    # --- 2.1 TRATAMENTO NUMÉRICO (CORRIGIDO) ---
    # Colunas que DEVEM ser 0.0 se forem nulas (ex: desmatamento não detectado = 0)
    zero_fill_cols = ['mapbiomas_deforested_ha', 'embargo_area_ha', 'property_area_ha', 'latitude', 'longitude']
    for col in zero_fill_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

    # Coluna de RL: Se for nula, mantemos NaN (dado não calculado), não forçamos 0.
    if 'rl_deficit_ha' in df.columns:
        df['rl_deficit_ha'] = pd.to_numeric(df['rl_deficit_ha'], errors='coerce')

    # --- 2.2 MAPEAMENTO DE STATUS (COMPLETO BASEADO NO SQL) ---
    def simplify_status(s):
        s = str(s).upper()
        
        # Bloqueios (Not Eligible)
        if 'SOCIAL' in s: return '⛔ Trabalho Escravo'
        if 'TRADITIONAL' in s: return '⛔ Terra Indígena/Quilombola'
        if 'CAR_STATUS' in s or 'CAR STATUS' in s: return '⛔ CAR Suspenso/Cancelado'
        if 'CMN_5081' in s: return '⛔ Embargo (CMN 5.081)'
        if 'IBAMA' in s: return '⛔ Embargo IBAMA'
        if 'MAPBIOMAS' in s: return '⛔ Desmatamento Recente'
        if 'CONSERVATION' in s: return '⛔ Unidade de Conservação'
        if 'SATELLITE' in s: return '⛔ Risco Satélite'
        
        # Avisos (Warnings)
        if 'ADJACENCY' in s: return '⚠️ Risco Vizinho'
        if 'OLD_EMBARGO' in s or 'OLD EMBARGO' in s: return '⚠️ Embargo Histórico'
        if 'BORDERLINE' in s: return '⚠️ Declividade Limite'
        
        # Outros
        if 'CLOUD' in s or 'AWAITING' in s: return '☁️ Obstruído por Nuvens'
        if 'RL_DEFICIT' in s or 'RL DEFICIT' in s: return '⚖️ Déficit Reserva Legal'
        
        if 'ELIGIBLE' in s: return '✅ Conforme'
        
        return s # Retorna o original se não casar com nada

    df['status_pt'] = df['final_eligibility_status'].apply(simplify_status)

    # Trabalho Escravo (Coluna Auxiliar)
    if 'slave_labor_offender' in df.columns:
        df['slave_labor_check'] = df['slave_labor_offender'].apply(
            lambda x: "⚠️ Lista Suja" if pd.notnull(x) and str(x).strip() not in ['', 'None', 'nan', '|'] else "✅ Regular"
        )
    
    # --- 2.3 LIMPEZA DA EVIDÊNCIA TÉCNICA ---
    def clean_evidence_string(text):
        if pd.isna(text) or str(text).strip() == "":
            return "Sem restrições."
        
        text = str(text)
        
        # 1. Remove bloco de vizinhança (poluição visual)
        text = re.sub(r'🚧 VIZINHANÇA:.*?(?=(📊|⚠️|$))', '', text, flags=re.DOTALL)
        
        # 2. Remove cabeçalhos internos
        text = text.replace('⚠️ RESTRIÇÕES:', '').replace('📊 MÉTRICAS:', '')
        
        # 3. Divide e limpa itens
        parts = text.split('|')
        clean_items = []
        
        for item in parts:
            item = item.strip()
            if not item: continue
            
            # IGNORA MENSAGEM DE SUCESSO DO SQL
            if "EM CONFORMIDADE" in item or "✅" in item:
                continue
            
            # Formata Chave: Valor
            if ':' in item:
                key, value = item.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # Simplifica nomes longos
                if 'Desmatamento MapBiomas' in key: key = 'Desmatamento'
                elif 'Embargo' in key: key = 'Embargo'
                elif 'Déficit de RL' in key: key = 'Déficit RL'
                elif 'Declividade' in key: key = 'Declividade'
                
                clean_items.append(f"• {key}: {value}")
            
            # Formata Flags de Risco
            elif len(item) > 3: 
                clean_items.append(f"• ⚠️ {item}")

        if not clean_items:
            return "✅ Parâmetros dentro dos limites legais."

        return "\n".join(clean_items)

    if 'technical_evidence' in df.columns:
        df['technical_evidence'] = df['technical_evidence'].apply(clean_evidence_string)
    else:
        df['technical_evidence'] = "Sem dados"

    # --- 2.4 LINK MAPBIOMAS ---
    def create_mapbiomas_link(alert_id):
        if pd.notnull(alert_id) and str(alert_id).strip() not in ['0', '0.0', '-', '', 'None']:
            try:
                clean_id = str(int(float(alert_id)))
                return f"https://plataforma.alerta.mapbiomas.org/alerta/{clean_id}"
            except:
                return None
        return None

    if 'mapbiomas_alert_id' in df.columns:
        df['mapbiomas_url'] = df['mapbiomas_alert_id'].apply(create_mapbiomas_link)
    else:
        df['mapbiomas_url'] = None

    return df

df = load_data()

# -----------------------------------------------------------------------------
# 3. SIDEBAR E FILTROS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown('<div class="sidebar-section-header">🔍 Busca Específica</div>', unsafe_allow_html=True)
    search_car = st.text_input("Código CAR (ID)", placeholder="Ex: MT-1234...")
    search_coords = st.text_input("Coordenadas (Lat, Lon)", placeholder="-12.55, -55.10")
    
    st.markdown("---")
    st.markdown('<div class="sidebar-section-header">📍 Filtros Gerais</div>', unsafe_allow_html=True)

    biomes = sorted(df['biome_name'].dropna().unique()) if 'biome_name' in df.columns else []
    sel_biome = st.multiselect("Bioma", biomes, placeholder="Todos")
    
    df_filtered = df.copy()
    if sel_biome: df_filtered = df_filtered[df_filtered['biome_name'].isin(sel_biome)]
        
    cities = sorted(df_filtered['city'].dropna().unique())
    sel_city = st.multiselect("Município", cities, placeholder="Selecione...")
    
    statuses = sorted(df['status_pt'].unique())
    sel_status = st.multiselect("Parecer de Compliance", statuses, placeholder="Todos")

    st.markdown('<div class="sidebar-section-header">🛰️ Detecção de Risco</div>', unsafe_allow_html=True)
    filter_mapbiomas = st.checkbox("Apenas com Alertas MapBiomas", value=False)
    filter_rl = st.checkbox("Apenas com Déficit de RL", value=False)

# -----------------------------------------------------------------------------
# 4. APLICAÇÃO DOS FILTROS
# -----------------------------------------------------------------------------
df_show = df_filtered.copy()

if search_car:
    df_show = df_show[df_show['property_id'].astype(str).str.contains(search_car, case=False, na=False)]

if search_coords:
    try:
        parts = search_coords.replace(';', ',').split(',')
        if len(parts) == 2:
            lat_input, lon_input = float(parts[0].strip()), float(parts[1].strip())
            tolerance = 0.01
            df_show = df_show[
                (df_show['latitude'].between(lat_input - tolerance, lat_input + tolerance)) &
                (df_show['longitude'].between(lon_input - tolerance, lon_input + tolerance))
            ]
    except:
        st.sidebar.error("Coordenadas inválidas.")

if sel_city: df_show = df_show[df_show['city'].isin(sel_city)]
if sel_status: df_show = df_show[df_show['status_pt'].isin(sel_status)]
if filter_mapbiomas: df_show = df_show[df_show['mapbiomas_deforested_ha'] > 0]
if filter_rl: df_show = df_show[df_show['rl_deficit_ha'] > 0]

# -----------------------------------------------------------------------------
# 5. DASHBOARD (KPIs)
# -----------------------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Passivo de Reserva Legal", f"{df_show['rl_deficit_ha'].sum():,.0f} ha")
c2.metric("Desmatamento (MapBiomas)", f"{df_show['mapbiomas_deforested_ha'].sum():,.0f} ha", delta_color="inverse")
c3.metric("Área Embargada (IBAMA)", f"{df_show['embargo_area_ha'].sum():,.0f} ha")

# Conta quantos começam com o ícone de bloqueio ⛔
blocked_count = df_show[df_show['status_pt'].str.contains('⛔')].shape[0]
c4.metric("Imóveis Bloqueados", f"{blocked_count}", delta_color="inverse")

st.markdown("---")

# -----------------------------------------------------------------------------
# 6. TABELA DETALHADA
# -----------------------------------------------------------------------------
st.markdown(f"### 📋 Detalhamento ({len(df_show)} imóveis listados)")

column_config = {
    "property_id": st.column_config.TextColumn("Código CAR", width="medium"),
    "city": st.column_config.TextColumn("Município"),
    "property_area_ha": st.column_config.NumberColumn("Área Total", format="%.0f ha"),
    "status_pt": st.column_config.TextColumn("Parecer", width="medium"),
    "technical_evidence": st.column_config.TextColumn("📝 Evidência Técnica", width="large"),
    
    # Configuração da Barra de Progresso para RL
    "rl_deficit_ha": st.column_config.ProgressColumn(
        "Déficit RL", 
        format="%.1f ha", 
        min_value=0, 
        # Define o máximo dinamicamente, mas protege contra DataFrame vazio
        max_value=float(df['rl_deficit_ha'].max()) if not df.empty and pd.notnull(df['rl_deficit_ha'].max()) else 100
    ),
    
    "mapbiomas_deforested_ha": st.column_config.NumberColumn("Desmat. (ha)", format="%.1f ha"),
    
    "mapbiomas_url": st.column_config.LinkColumn(
        "Laudo MapBiomas",
        display_text="📄 Abrir Laudo",
        help="Acesse o relatório oficial do alerta de desmatamento.",
        width="small"
    ),
    
    "latitude": st.column_config.NumberColumn("Lat", format="%.5f"),
    "longitude": st.column_config.NumberColumn("Lon", format="%.5f"),
    "slave_labor_check": st.column_config.TextColumn("Lista Suja")
}

cols_to_show = [
    'property_id', 'status_pt', 'technical_evidence', 'city', 'property_area_ha',
    'rl_deficit_ha', 'mapbiomas_deforested_ha', 
    'mapbiomas_url',
    'latitude', 'longitude', 'slave_labor_check'
]

cols_final = [c for c in cols_to_show if c in df_show.columns]

st.dataframe(
    df_show[cols_final].sort_values(by='mapbiomas_deforested_ha', ascending=False),
    column_config=column_config,
    use_container_width=True,
    height=600,
    hide_index=True
)

st.divider() # Uma linha sutil para separar o conteúdo do rodapé
st.caption(f"Desenvolvido por **Raphael Soares** | **Agri-Market Intelligence & Risk Automation**")
st.caption(f"📅 Dados processados em: {pd.Timestamp.now().strftime('%d/%m/%Y')}")
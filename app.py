import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Command Center 2026", layout="wide")

# --- ESTILOS PERSONALIZADOS ---
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- CARGA DE DATOS ---
@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        archivos_permitidos = ['Maestro_Productos', 'Sell_In_Ventas', 'Sell_Out', 'Stock', 'Ingresos']
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            if name in archivos_permitidos:
                request = service.files().get_media(fileId=f['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done: _, done = downloader.next_chunk()
                fh.seek(0)
                df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
                df.columns = [str(c).strip().upper() for c in df.columns]
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU'})
                if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return {}

data = load_drive_data()

if data:
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Procesamiento de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), 'FECHA')
            df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
            df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')
            df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR: FILTROS ESTRATÉGICOS ---
    st.sidebar.title("🎮 CONTROLES")
    f_emp = st.sidebar.selectbox("EMPRENDIMIENTO", ["TODOS"] + list(sell_in['EMPRENDIMIENTO'].unique() if 'EMPRENDIMIENTO' in sell_in.columns else []))
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("PROYECCIÓN 2026")
    growth_rate = st.sidebar.slider("% Crecimiento s/ 2025", -50, 150, 20)
    
    # Filtros Globales
    st.sidebar.subheader("FILTROS MIX")
    f_disc = st.sidebar.multiselect("Disciplina", maestro['DISCIPLINA'].unique() if 'DISCIPLINA' in maestro.columns else [])
    f_gen = st.sidebar.multiselect("Género", maestro['GENERO'].unique() if 'GENERO' in maestro.columns else [])
    
    # Aplicar Filtros
    m_filt = maestro.copy()
    if f_disc: m_filt = m_filt[m_filt['DISCIPLINA'].isin(f_disc)]
    if f_gen: m_filt = m_filt[m_filt['GENERO'].isin(f_gen)]
    
    # --- KPIs DE CABECERA ---
    st.title("👟 FILA - Torre de Control Estratégica")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    total_stk = stock[stock['SKU'].isin(m_filt['SKU'])]['CANTIDAD'].sum() if not stock.empty else 0
    total_si_25 = sell_in[(sell_in['SKU'].isin(m_filt['SKU'])) & (sell_in['AÑO'] == 2025)]['UNIDADES'].sum()
    total_so_25 = sell_out[(sell_out['SKU'].isin(m_filt['SKU'])) & (sell_out['AÑO'] == 2025)]['CANTIDAD'].sum()
    
    kpi1.metric("Stock Actual", f"{total_stk:,.0f} u")
    kpi2.metric("Sell In 2025", f"{total_si_25:,.0f} u")
    kpi3.metric("Sell Out 2025", f"{total_so_25:,.0f} u")
    kpi4.metric("Eficiencia Sell Out", f"{(total_so_25/total_si_25*100):.1f}%" if total_si_25 > 0 else "0%")

    # --- TABS ---
    t1, t2, t3 = st.tabs(["📊 PERFORMANCE & MIX", "⚡ TACTICAL & MOS", "🔮 PROYECCIONES 2026"])

    with t1:
        st.subheader("Análisis de Demanda y Salud de Inventario")
        c1, c2 = st.columns([2, 1])
        
        with c1:
            # Línea de tiempo Sell In vs Sell Out
            si_time = sell_in[sell_in['SKU'].isin(m_filt['SKU'])].groupby('MES_KEY')['UNIDADES'].sum().reset_index()
            so_time = sell_out[sell_out['SKU'].isin(m_filt['SKU'])].groupby('MES_KEY')['CANTIDAD'].sum().reset_index()
            fig_vta = go.Figure()
            fig_vta.add_trace(go.Scatter(x=si_time['MES_KEY'], y=si_time['UNIDADES'], name="Sell In (Dass)", line=dict(color='#1f77b4', width=3)))
            fig_vta.add_trace(go.Scatter(x=so_time['MES_KEY'], y=so_time['CANTIDAD'], name="Sell Out (Mercado)", line=dict(color='#ff7f0e', width=3, dash='dot')))
            fig_vta.update_layout(title="Curva de Ventas Histórica", hovermode="x unified")
            st.plotly_chart(fig_vta, use_container_width=True)
            
        with c2:
            # Mix por Disciplina
            mix_data = sell_out[sell_out['SKU'].isin(m_filt['SKU'])].merge(maestro[['SKU', 'DISCIPLINA']], on='SKU')
            fig_mix = px.pie(mix_data, values='CANTIDAD', names='DISCIPLINA', title="Participación de Sell Out", hole=.4)
            st.plotly_chart(fig_mix, use_container_width=True)

    with t2:
        st.subheader("Months of Stock y Ranking de Productos")
        # Lógica MOS avanzada
        vta_prom_25 = sell_out[(sell_out['SKU'].isin(m_filt['SKU'])) & (sell_out['AÑO'] == 2025)].groupby('SKU')['CANTIDAD'].mean()
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum()
        
        ranking = m_filt.merge(stk_sku, on='SKU', how='left').merge(vta_prom_25, on='SKU', how='left').fillna(0)
        ranking.columns = ['SKU', 'DESCRIPCION', 'DISCIPLINA', 'GENERO', 'FRANJA', 'STOCK', 'VTA_PROM_25']
        ranking['VTA_PROY_26'] = ranking['VTA_PROM_25'] * (1 + growth_rate/100)
        ranking['MOS'] = (ranking['STOCK'] / ranking['VTA_PROY_26']).replace([float('inf')], 99).round(1)
        
        # Clasificación
        def classify(row):
            if row['MOS'] < 2: return 'Crítico (Quiebre)'
            if row['MOS'] > 8: return 'Exceso (Sobre-stock)'
            return 'Saludable'
        ranking['ESTADO'] = ranking.apply(classify, axis=1)
        
        st.dataframe(ranking.sort_values('VTA_PROY_26', ascending=False), use_container_width=True)

    with t3:
        st.subheader("Simulador de Disponibilidad 2026")
        sku_sel = st.selectbox("Seleccionar Producto Crítico", m_filt['SKU'].unique())
        
        if sku_sel:
            # Construcción de la Línea de Tiempo de Oportunidad
            meses_26 = pd.date_range(start='2026-01-01', periods=12, freq='MS').strftime('%Y-%m')
            
            # Datos
            stk_actual = stock[stock['SKU'] == sku_sel]['CANTIDAD'].sum()
            vta_base = sell_out[(sell_out['SKU'] == sku_sel) & (sell_out['AÑO'] == 2025)]['CANTIDAD'].mean()
            vta_p = vta_base * (1 + growth_rate/100)
            ings = ingresos[ingresos['SKU'] == sku_sel].groupby('MES_KEY')['UNIDADES'].sum()
            
            # Evolución
            stk_evo = []
            c_stk = stk_actual
            for m in meses_26:
                c_stk = c_stk + ings.get(m, 0) - vta_p
                stk_evo.append(max(0, c_stk))
            
            fig_proj = go.Figure()
            fig_proj.add_trace(go.Bar(x=meses_26, y=[ings.get(m, 0) for m in meses_26], name="Ingresos Planificados", marker_color='#2ecc71', opacity=0.6))
            fig_proj.add_trace(go.Scatter(x=meses_26, y=stk_evo, name="Stock Proyectado", line=dict(color='#e74c3c', width=4)))
            fig_proj.add_hline(y=vta_p * 2, line_dash="dash", line_color="gray", annotation_text="Stock de Seguridad (2 meses)")
            
            fig_proj.update_layout(title=f"Proyección de Abastecimiento para {sku_sel}", xaxis_title="Meses 2026", yaxis_title="Unidades")
            st.plotly_chart(fig_proj, use_container_width=True)
            
            st.warning(f"💡 Al ritmo proyectado, el producto tendrá un stock promedio de {sum(stk_evo)/12:.0f} unidades en 2026.")

else:
    st.info("Configurá los secretos y subí los archivos al Drive para activar la Torre de Control.")

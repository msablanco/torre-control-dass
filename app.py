import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control Forecast", layout="wide")

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
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_SI'})
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

    # Normalización de Fechas y Nombres de Cliente
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m') # Para ordenar por mes sin importar el año
                df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR: MEGA FILTROS ---
    st.sidebar.title("🎮 PARÁMETROS")
    
    # 1. Buscador SKU / Descripción
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Producto", "").upper()
    
    # 2. % Crecimiento s/ Sell Out 2025
    growth_rate = st.sidebar.slider("% Var. Sell Out 2026 vs 2025", -100, 150, 0)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("FILTROS DE CANAL Y CLIENTE")
    
    f_emp = st.sidebar.multiselect("Emprendimiento", sell_in['EMPRENDIMIENTO'].unique() if 'EMPRENDIMIENTO' in sell_in.columns else [])
    f_cli_si = st.sidebar.multiselect("Sell In Clientes", sell_in['CLIENTE_SI'].unique() if 'CLIENTE_SI' in sell_in.columns else [])
    f_cli_so = st.sidebar.multiselect("Sell Out Clientes (Canal)", sell_out['CLIENTE'].unique() if 'CLIENTE' in sell_out.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query:
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja:
        m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli_si: si_filt = si_filt[si_filt['CLIENTE_SI'].isin(f_cli_si)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_cli_so: so_filt = so_filt[so_filt['CLIENTE'].isin(f_cli_so)]

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS SKU"])

    with tab1:
        st.subheader("Curva de Demanda: Real vs Proyectado")
        
        # Agrupamos por mes (formato '01', '02', etc.) para comparar estacionalidad
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        # Crear la línea de Proyección Sell Out 2026
        so_25['PROY_2026'] = so_25['CANTIDAD'] * (1 + growth_rate/100)
        
        # Mapeo de meses para el eje X
        meses_nombres = {
            '01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun',
            '07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'
        }
        so_25['MES_NOM'] = so_25['MES_STR'].map(meses_nombres)
        si_25['MES_NOM'] = si_25['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        # Sell In 2025 (Referencia)
        fig.add_trace(go.Scatter(x=si_25['MES_NOM'], y=si_25['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        # Sell Out 2025 (Base)
        fig.add_trace(go.Scatter(x=so_25['MES_NOM'], y=so_25['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        # PROYECCIÓN 2026 (Nueva Línea)
        fig.add_trace(go.Scatter(x=so_25['MES_NOM'], y=so_25['PROY_2026'], name="Proyección Sell Out 2026", line=dict(color='#2ecc71', width=4)))
        
        fig.update_layout(title="Análisis Estacional y Forecast", hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Velocidad de Stock Proyectada")
        vta_ref = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        stk_act = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        
        ranking = m_filt.merge(stk_act, on='SKU', how='left').merge(vta_ref, on='SKU', how='left').fillna(0)
        ranking['VTA_PROY_26'] = (ranking['VTA_25'] * (1 + growth_rate/100)).round(0)
        ranking['MOS'] = (ranking['STK'] / ranking['VTA_PROY_26']).replace([float('inf')], 99).round(1)
        
        st.dataframe(ranking.sort_values('VTA_PROY_26', ascending=False), use_container_width=True)

    with tab3:
        # Aquí va la misma lógica de proyección pero a nivel SKU individual
        sku_sel = st.selectbox("Elegir Producto para análisis de agotamiento", m_filt['SKU'].unique())
        # (Lógica de proyección individual similar a la anterior pero filtrada por SKU)
        st.info(f"Análisis enfocado en: {sku_sel}")

else:
    st.info("Esperando archivos...")

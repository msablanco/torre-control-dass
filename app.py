import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Command Center 2026", layout="wide")

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

    # Normalización de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR ---
    st.sidebar.title("🎮 FILTROS")
    growth_rate = st.sidebar.slider("% Crecimiento s/ 2025", -50, 150, 20)
    
    m_filt = maestro.copy()
    if not m_filt.empty:
        if 'DISCIPLINA' in m_filt.columns:
            f_disc = st.sidebar.multiselect("Disciplina", m_filt['DISCIPLINA'].unique())
            if f_disc: m_filt = m_filt[m_filt['DISCIPLINA'].isin(f_disc)]
        if 'GENERO' in m_filt.columns:
            f_gen = st.sidebar.multiselect("Género", m_filt['GENERO'].unique())
            if f_gen: m_filt = m_filt[m_filt['GENERO'].isin(f_gen)]

    # --- TABS ---
    t1, t2, t3 = st.tabs(["📊 ESTRATEGIA", "⚡ TACTICAL & MOS", "🔮 PROYECCIÓN 2026"])

    with t1:
        st.subheader("Performance Sell In vs Sell Out")
        c1, c2 = st.columns([2, 1])
        with c1:
            si_t = sell_in[sell_in['SKU'].isin(m_filt['SKU'])].groupby('MES_KEY')['UNIDADES'].sum().reset_index()
            so_t = sell_out[sell_out['SKU'].isin(m_filt['SKU'])].groupby('MES_KEY')['CANTIDAD'].sum().reset_index()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=si_t['MES_KEY'], y=si_t['UNIDADES'], name="Sell In", line=dict(color='#1f77b4', width=3)))
            fig.add_trace(go.Scatter(x=so_t['MES_KEY'], y=so_t['CANTIDAD'], name="Sell Out", line=dict(color='#ff7f0e', dash='dot')))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            if not sell_out.empty and 'DISCIPLINA' in m_filt.columns:
                mix = sell_out[sell_out['SKU'].isin(m_filt['SKU'])].merge(m_filt[['SKU', 'DISCIPLINA']], on='SKU')
                st.plotly_chart(px.pie(mix, values='CANTIDAD', names='DISCIPLINA', hole=.4), use_container_width=True)

    with t2:
        st.subheader("Ranking MOS (Months of Stock)")
        # Cálculo de MOS Dinámico (Evitando el error de columnas)
        vta_25 = sell_out[sell_out['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index().rename(columns={'CANTIDAD': 'VTA_PROM'})
        stk_s = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_ACTUAL'})
        
        # Merge Seguro
        ranking = m_filt.merge(stk_s, on='SKU', how='left').merge(vta_25, on='SKU', how='left').fillna(0)
        
        # Proyectar venta 2026
        ranking['VTA_PROY_26'] = (ranking['VTA_PROM'] * (1 + growth_rate/100)).round(0)
        ranking['MOS'] = (ranking['STOCK_ACTUAL'] / ranking['VTA_PROY_26']).replace([float('inf')], 99).round(1)
        
        # Mostrar solo columnas útiles
        cols_show = ['SKU', 'DESCRIPCION', 'DISCIPLINA', 'STOCK_ACTUAL', 'VTA_PROY_26', 'MOS']
        cols_present = [c for c in cols_show if c in ranking.columns]
        st.dataframe(ranking[cols_present].sort_values('VTA_PROY_26', ascending=False), use_container_width=True)

    with t3:
        st.subheader("Simulador de Disponibilidad 2026")
        sku_sel = st.selectbox("Seleccionar SKU", m_filt['SKU'].unique())
        if sku_sel:
            meses_26 = pd.date_range(start='2026-01-01', periods=12, freq='MS').strftime('%Y-%m')
            stk_ini = stock[stock['SKU'] == sku_sel]['CANTIDAD'].sum()
            vta_base = sell_out[(sell_out['SKU'] == sku_sel) & (sell_out['AÑO'] == 2025)]['CANTIDAD'].mean()
            vta_p = (vta_base if not pd.isna(vta_base) else 0) * (1 + growth_rate/100)
            ings = ingresos[ingresos['SKU'] == sku_sel].groupby('MES_KEY')['UNIDADES'].sum()
            
            stk_e = []
            curr = stk_ini
            for m in meses_26:
                curr = curr + ings.get(m, 0) - vta_p
                stk_e.append(max(0, curr))
            
            fig_p = go.Figure()
            fig_p.add_trace(go.Bar(x=meses_26, y=[ings.get(m,0) for m in meses_26], name="Arribos 2026", marker_color='green', opacity=0.5))
            fig_p.add_trace(go.Scatter(x=meses_26, y=stk_e, name="Stock Proyectado", line=dict(color='red', width=4)))
            st.plotly_chart(fig_p, use_container_width=True)
else:
    st.info("Subí los archivos al Drive para iniciar.")

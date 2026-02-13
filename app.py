import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control Forecast", layout="wide")

# --- CARGA DE DATOS (INTOCABLE) ---
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
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_NAME'})
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
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR: FILTROS UNIFICADOS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique()))) if 'EMPRENDIMIENTO' in sell_in.columns else []
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        # Tu lógica de Tab 1 se mantiene igual
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index() if not si_filt.empty else pd.DataFrame(columns=['MES_STR', 'UNIDADES'])
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index() if not so_filt.empty else pd.DataFrame(columns=['MES_STR', 'CANTIDAD'])
        
        total_so_25 = so_25['CANTIDAD'].sum() if not so_25.empty else 0
        if total_so_25 > 0:
            so_25['PROY_2026'] = ((so_25['CANTIDAD'] / total_so_25) * target_vol).round(0)
        else:
            so_25['PROY_2026'] = 0

        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True, key="chart_performance_main")

        st.markdown("### 📋 Detalle Mensual")
        df_t1 = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].set_index('MES_NOM').T
        df_t1['TOTAL'] = df_t1.sum(axis=1)
        st.dataframe(df_t1.style.format("{:,.0f}"), use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        # Lógica Tactical: Stock Actual vs Promedio Venta
        if not stock.empty:
            stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
            # Promedio venta mensual 2025 (Sell Out)
            so_avg = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index().rename(columns={'CANTIDAD':'VTA_PROM'})
            
            tactical = m_filt.merge(stk_sku, on='SKU', how='left').merge(so_avg, on='SKU', how='left').fillna(0)
            tactical['MOS'] = (tactical['CANTIDAD'] / tactical['VTA_PROM']).replace([float('inf'), -float('inf')], 0).fillna(0)
            
            st.dataframe(tactical[['SKU', 'DESCRIPCION', 'CANTIDAD', 'VTA_PROM', 'MOS']].style.format({'VTA_PROM': '{:.1f}', 'MOS': '{:.1f}'}), use_container_width=True)
        else:
            st.warning("No se detectaron datos de Stock para calcular el MOS.")

    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        if not m_filt.empty:
            sku_sel = st.selectbox("Seleccionar SKU para Escenario", m_filt['SKU'].unique(), key="sel_tab3")
            
            # Cálculo simple de proyección de stock (Stock - Venta Proyectada)
            stk_actual = stock[stock['SKU']==sku_sel]['CANTIDAD'].sum() if not stock.empty else 0
            vta_mensual_proy = df_plot['PROY_2026'] # Usamos la curva del target vol
            
            fig_stk = go.Figure()
            # Aquí podrías agregar la lógica de stock evolutivo
            fig_stk.add_trace(go.Bar(x=df_plot['MES_NOM'], y=vta_mensual_proy, name="Salida Proyectada 2026"))
            fig_stk.update_layout(title=f"Demanda Esperada 2026 para {sku_sel}")
            st.plotly_chart(fig_stk, use_container_width=True, key="chart_tab3_evol")

else:
    st.info("Esperando conexión con Google Drive...")

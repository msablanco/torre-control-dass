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
                # Normalización de nombres de columnas comunes
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
    # El filtro de emprendimiento ahora afecta a ambos archivos
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    # Aplicamos filtro de emprendimiento y cliente a Sell In
    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    # Aplicamos filtro de emprendimiento y cliente a Sell Out
    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

# --- 4. ESTRUCTURA DE PESTAÑAS ---
    # Es vital que aquí haya 3 nombres para que 'tab3' exista
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # =========================================================
    # SOLAPA 1: PERFORMANCE & PROYECCIÓN (INTOCABLE)
    # =========================================================
    with tab1:
        # ... (Aquí va todo tu código de la Tab 1 que ya funciona perfecto) ...
        # Asegúrate de mantener tu lógica de si_25, so_25 y el gráfico fig
        st.subheader("Análisis de Demanda y Proyección Unificada")
        
        # (Copia aquí tu lógica actual de la Tab 1 para que no cambie nada)
        # Suponiendo que df_plot y target_vol vienen de tu lógica anterior:
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index() if 'UNIDADES' in si_filt.columns else pd.DataFrame()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index() if 'CANTIDAD' in so_filt.columns else pd.DataFrame()
        
        total_so_25 = so_25['CANTIDAD'].sum() if not so_25.empty else 0
        so_25['PROY_2026'] = ((so_25['CANTIDAD'] / total_so_25) * target_vol).round(0) if total_so_25 > 0 else 0

        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True)
        # ... (resto de tus tablas de la Tab 1) ...

    # =========================================================
    # SOLAPA 2: TACTICAL (MATRIZ DE SALUD - MOS)
    # =========================================================
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        if not stock.empty:
            # Consolidamos Stock por SKU
            stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_ACTUAL'})
            
            # Calculamos Venta Promedio Mensual Proyectada 2026 para el MOS
            vta_prom_sku = (target_vol / 12) / len(m_filt['SKU'].unique()) if len(m_filt) > 0 else 1
            
            # Unimos Maestro con Stock
            tactical = m_filt[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(stk_sku, on='SKU', how='left').fillna(0)
            tactical['MOS'] = (tactical['STOCK_ACTUAL'] / vta_prom_sku).round(1)
            
            # Color por criticidad
            def color_critico(val):
                color = '#ff4b4b' if val < 1 else '#ffa500' if val < 2 else '#00cc66'
                return f'color: {color}'

            st.dataframe(tactical.style.applymap(color_critico, subset=['MOS']), use_container_width=True)
        else:
            st.warning("Cargue el archivo de Stock para ver la Matriz de Salud.")

    # =========================================================
    # SOLAPA 3: ESCENARIOS (LÍNEA DE TIEMPO DE OPORTUNIDAD)
    # =========================================================
    with tab3:
        st.subheader("🔮 Línea de Tiempo Dinámica de Oportunidad")
        if not m_filt.empty:
            sku_sel = st.selectbox("Seleccionar SKU para análisis 360", m_filt['SKU'].unique())
            
            # Simulación de agotamiento
            stk_ini = stock[stock['SKU'] == sku_sel]['CANTIDAD'].sum() if not stock.empty else 0
            curva_vta = df_plot['PROY_2026'].values / len(m_filt['SKU'].unique()) # Proporción estimada
            
            stk_evol = []
            temp_stk = stk_ini
            for v in curva_vta:
                temp_stk = max(0, temp_stk - v)
                stk_evol.append(temp_stk)
            
            fig_evol = go.Figure()
            fig_evol.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=stk_evol, fill='tozeroy', name="Stock Proyectado"))
            fig_evol.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=curva_vta, name="Venta Mensual", line=dict(dash='dot')))
            st.plotly_chart(fig_evol, use_container_width=True)

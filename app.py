import streamlit as st
import pandas as pd
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
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper() for c in df.columns]
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_NAME'})
            if 'SKU' in df.columns: 
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return {}

data = load_drive_data()

if data:
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Formateo de fechas
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- SIDEBAR: PARÁMETROS DE CÁLCULO ---
    st.sidebar.title("🔒 BLINDAJE DE PROYECCIÓN")
    target_vol = st.sidebar.number_input("Volumen Objetivo 2026", value=700000, step=50000)
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp_calc = st.sidebar.multiselect("Canal para base de cálculo", opciones_emp)

    # BOTÓN DE BLINDAJE
    if st.sidebar.button("🔒 CALCULAR Y BLINDAR PROYECCIÓN"):
        so_ref = sell_out[sell_out['AÑO'] == 2025].copy()
        if f_emp_calc:
            so_ref = so_ref[so_ref['EMPRENDIMIENTO'].isin(f_emp_calc)]
        
        venta_base = so_ref['CANTIDAD'].sum()
        st.session_state['factor_blindado'] = target_vol / venta_base if venta_base > 0 else 1
        st.sidebar.success(f"Factor Blindado: {st.session_state['factor_blindado']:.4f}")

    # --- FILTROS DE VISTA (NO AFECTAN AL CÁLCULO SI ESTÁ BLINDADO) ---
    st.sidebar.markdown("---")
    st.sidebar.title("🔍 FILTROS DE VISTA")
    search_query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()
    
    # Usar el factor blindado o uno temporal
    factor_final = st.session_state.get('factor_blindado', 1.0)

    # --- PROCESAMIENTO DE TABLA TACTICAL ---
    m_filt = maestro.copy()
    if search_query:
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]

    # Agrupar Sell Out 2025 (filtrado por canal seleccionado arriba)
    so_25 = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp_calc:
        so_25 = so_25[so_25['EMPRENDIMIENTO'].isin(f_emp_calc)]
    
    vta_sku = so_25[so_25['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
    stk_sku = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    ing_sku = ingresos[ingresos['SKU'].isin(m_filt['SKU'])].groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS'})

    # Unión Final (Deduplicada)
    tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_sku, on='SKU', how='left') \
                     .merge(vta_sku, on='SKU', how='left') \
                     .merge(ing_sku, on='SKU', how='left').fillna(0)

    # CÁLCULOS BLINDADOS
    tactical['VTA_PROY_2026'] = (tactical['VTA_25'] * factor_final).round(0)
    tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    
    # MOS: Evitar infinito y -inf
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # --- INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)"])

    with tab2:
        if 'factor_blindado' not in st.session_state:
            st.warning("⚠️ La proyección no está blindada. Presiona el botón en la barra lateral para fijar el cálculo.")
        
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # Filtro de limpieza para la vista
        df_display = tactical[(tactical['STOCK'] > 0) | (tactical['VTA_25'] > 0) | (tactical['INGRESOS'] > 0)]
        
        # KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs en Pantalla", len(df_display))
        c2.metric("Factor de Crecimiento", f"{factor_final:.2%}")
        avg_mos = df_display[df_display['VTA_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("MOS Promedio", f"{avg_mos:.1f} meses" if not pd.isna(avg_mos) else "0.0")

        st.dataframe(df_display[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_25', 'INGRESOS', 'VTA_MENSUAL', 'MOS']]
                     .sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)

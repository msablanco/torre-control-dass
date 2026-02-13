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
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_NAME'})
                if 'SKU' in df.columns: 
                    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return {}

data = load_drive_data()

if data:
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())

    # Formateo de fechas
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- SIDEBAR ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 100000, 2000000, 700000, step=50000)
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

    # --- 1. CÁLCULO DEL DENOMINADOR ESTÁTICO (EL TRUCO) ---
    # Calculamos la venta 2025 de TODO el canal seleccionado, IGNORANDO el buscador de SKU
    so_canal_full = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp:
        so_canal_full = so_canal_full[so_canal_full['EMPRENDIMIENTO'].isin(f_emp)]
    
    venta_base_canal = so_canal_full['CANTIDAD'].sum()
    # Factor de escala único para el canal
    FACTOR_REAL = target_vol / venta_base_canal if venta_base_canal > 0 else 1

    # --- 2. FILTRADO PARA LA TABLA (VISTA) ---
    m_filt = maestro.copy()
    if search_query: 
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    
    # Agrupamos por SKU para eliminar duplicados
    stk_sku = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku = so_canal_full[so_canal_full['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})

    # Unión de tabla Tactical
    tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_sku, on='SKU', how='left').merge(vta_sku, on='SKU', how='left').fillna(0)
    
    # Cálculos basados en el factor real del CANAL
    tactical['VTA_PROY_2026'] = (tactical['VTA_25'] * FACTOR_REAL).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    
    # MOS corregido contra -inf
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)
    
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # KPIs Superiores
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs Filtrados", len(tactical))
        
        promedio_mos = tactical[tactical['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("Stock Promedio (MOS)", f"{promedio_mos:.1f} meses" if not pd.isna(promedio_mos) else "0.0 meses")

        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_25', 'VTA_PROY_MENSUAL', 'MOS']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Línea de Tiempo")
        # Definir tactical antes evita el NameError
        st.write("Selecciona un SKU en la tabla para ver su detalle.")

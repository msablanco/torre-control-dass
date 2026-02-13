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
        st.error(f"Error en carga: {e}")
        return {}

data = load_drive_data()

if data:
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR: PARÁMETROS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- 1. CÁLCULO DEL DENOMINADOR BLINDADO (SOLUCIÓN AL RECALCULO) ---
    # Calculamos el total de ventas 2025 SIN aplicar el filtro de búsqueda/SKU
    so_ref = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp:
        so_ref = so_ref[so_ref['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli:
        so_ref = so_ref[so_ref['CLIENTE_NAME'].isin(f_cli)]
    
    # Este número es el total real del canal, no cambia al buscar un SKU
    vta_tot_estatica = so_ref['CANTIDAD'].sum()
    FACTOR_FIJO = target_vol / vta_tot_estatica if vta_tot_estatica > 0 else 1

    # --- 2. FILTRADO PARA VISUALIZACIÓN ---
    m_filt = maestro.copy()
    if search_query: 
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: 
        m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    # Agrupamos datos para eliminar duplicados de SKUs en las tablas
    vta_sku = so_ref[so_ref['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
    stk_sku = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
    
    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # Definimos tactical fuera para evitar NameError en Tab 3
    tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_sku, on='SKU', how='left') \
                     .merge(vta_sku, on='SKU', how='left').fillna(0)
    
    # Cálculos con factor blindado
    tactical['VTA_PROY_ANUAL'] = (tactical['VTA_25'] * FACTOR_FIJO).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
    
    # Corrección de -inf meses
    tactical['MOS'] = (tactical['STK_ACTUAL'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 99).fillna(0).round(1)
    tactical['ESTADO'] = tactical.apply(lambda r: "🔥 QUIEBRE" if r['MOS'] < 2.5 else ("⚠️ SOBRE-STOCK" if r['MOS'] > 8 else "✅ SALUDABLE"), axis=1)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # Filtro final para no mostrar filas vacías
        df_tab2 = tactical[(tactical['STK_ACTUAL'] > 0) | (tactical['VTA_25'] > 0)]
        
        # KPIs Superiores
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs en Riesgo", len(df_tab2[df_tab2['ESTADO'] == "🔥 QUIEBRE"]))
        c2.metric("SKUs con Exceso", len(df_tab2[df_tab2['ESTADO'] == "⚠️ SOBRE-STOCK"]))
        
        avg_mos = df_tab2[df_tab2['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("Stock Promedio (MOS)", f"{avg_mos:.1f} meses" if not pd.isna(avg_mos) else "0.0 meses")

        st.dataframe(df_tab2[['SKU', 'DESCRIPCION', 'STK_ACTUAL', 'VTA_25', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_list = tactical[tactical['VTA_PROY_MENSUAL'] > 0]['SKU'].unique()
        if len(sku_list) > 0:
            sku_sel = st.selectbox("Seleccionar SKU", sku_list)
            # Lógica de proyección...
        else:
            st.warning("No hay datos de proyección para los filtros seleccionados.")

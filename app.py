import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="FILA - Forecast Control", layout="wide")

# --- 2. SIDEBAR (CONTROL DE VOLUMEN) ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")
vol_obj = st.sidebar.number_input("Volumen Total Objetivo 2026", value=1000000, step=50000)

# El botón que ya logramos visualizar
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS DE VISTA")
query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

# --- 3. FUNCIÓN DE CARGA ---
@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        query_drive = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query_drive, fields="files(id, name)").execute()
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
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_drive_data()

if data:
    # Preparación de Dataframes
    sell_out = data.get('Sell_Out', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year
        sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month

    # --- 4. LÓGICA DE BLINDAJE ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    total_empresa_2025 = so_2025['CANTIDAD'].sum()

    if validar_fijar:
        factor_escala = vol_obj / total_empresa_2025 if total_empresa_2025 > 0 else 1
        st.sidebar.success("🔒 Escala Fija Activa")
    else:
        df_ref = so_2025.copy()
        if query: df_ref = df_ref[df_ref['SKU'].str.contains(query)]
        v_ref = df_ref['CANTIDAD'].sum()
        factor_escala = vol_obj / v_ref if v_ref > 0 else 1
        st.sidebar.warning("⚠️ Escala Dinámica")

    # --- 5. CÁLCULO DE PROYECCIÓN MENSUAL ---
    meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    # Agrupamos ventas 2025 por mes
    ventas_mes = so_2025.groupby('MES_NUM')['CANTIDAD'].sum().reindex(range(1, 13), fill_value=0)
    
    # Si hay búsqueda, filtramos la serie temporal
    if query:
        so_filtrado = so_2025[so_2025['SKU'].str.contains(query) | so_2025['DESCRIPCION'].str.contains(query)]
        ventas_mes = so_filtrado.groupby('MES_NUM')['CANTIDAD'].sum().reindex(range(1, 13), fill_value=0)

    # Calculamos la línea de proyección 2026
    proy_2026 = (ventas_mes * factor_escala).round(0)

    # --- 6. INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)"])

    with tab1:
        st.subheader("📈 Curva de Proyección 2026")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses, y=ventas_mes, name="Sell Out 2025", line=dict(dash='dot')))
        fig.add_trace(go.Scatter(x=meses, y=proy_2026, name="Proyección 2026", line=dict(width=4, color='green')))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Detalle de Valores Mensuales")
        df_tabla = pd.DataFrame({
            "Mes": meses,
            "Sell Out 2025": ventas_mes.values,
            "Proyección 2026": proy_2026.values
        }).set_index("Mes").T
        st.dataframe(df_tabla, use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
        vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['VTA_PROY_26'] = (tactical['VTA_25'] * factor_escala).round(0)
        tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

        if query:
            tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query)]
        
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_25', 'VTA_MENSUAL', 'MOS']]
                     .sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)

else:
    st.info("Conectando con Drive...")

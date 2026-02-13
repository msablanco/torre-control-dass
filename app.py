import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# 1. CONFIGURACIÓN INICIAL DE LA APP
st.set_page_config(page_title="FILA - Forecast Control", layout="wide")

# --- SIDEBAR: PARÁMETROS (Esto se dibuja primero para que sea visible) ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")

# Ingreso del Objetivo
vol_obj = st.sidebar.number_input("Volumen Total Objetivo 2026", value=1000000, step=50000)

# EL CUADRO DE VALIDACIÓN (CHECKBOX)
# Al tildar esto, "congelamos" el cálculo sobre el total de la empresa.
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS DE VISTA")
query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

# --- 2. CARGA DE DATOS DESDE GOOGLE DRIVE ---
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
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper() for c in df.columns]
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU'})
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en la conexión con Drive: {e}")
        return {}

data = load_drive_data()

# --- 3. PROCESAMIENTO SI HAY DATOS ---
if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    # Procesar fechas de venta
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        if col_f:
            sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
            sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- 4. LÓGICA DE ESCALAMIENTO (EL "BLINDAJE") ---
    # Obtenemos la venta real total de 2025 (Denominador Inmóvil)
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy() if not sell_out.empty else pd.DataFrame()
    venta_total_empresa = so_2025['CANTIDAD'].sum() if not so_2025.empty else 0

    if validar_fijar:
        # SI ESTÁ TILDADO: El factor se calcula sobre el total de la empresa.
        # Es decir: (Objetivo 1M) / (Venta Total 2025). El resultado es un factor (ej: 1.05)
        # Este factor NO CAMBIA aunque filtres por un solo SKU.
        factor_escala = vol_obj / venta_total_empresa if venta_total_empresa > 0 else 1
        st.sidebar.success(f"🔒 ESCALA FIJADA: {factor_escala:.4f}")
    else:
        # SI NO ESTÁ TILDADO: Se comporta de forma dinámica (lo que generaba el error)
        # Calcula el factor basado SOLO en lo que se ve en pantalla.
        df_temp = so_2025.copy()
        if query:
            df_temp = df_temp[df_temp['SKU'].str.contains(query)]
        
        v_referencia = df_temp['CANTIDAD'].sum() if not df_temp.empty else 0
        factor_escala = vol_obj / v_referencia if v_referencia > 0 else 1
        st.sidebar.warning("⚠️ ESCALA DINÁMICA")

    # --- 5. UNIÓN DE TABLAS (TACTICAL) ---
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})

    # Combinamos todo en un DataFrame maestro
    tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
    
    # Aplicamos los cálculos de proyección
    tactical['VTA_PROY_2026'] = (tactical['VTA_2025'] * factor_escala).round(0)
    tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    
    # Cálculo de MOS (Meses de Stock)
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # Filtrar el DataFrame final según la búsqueda del Sidebar
    if query:
        tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query)]

    # --- 6. INTERFAZ FINAL ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        st.info("Nota: Si 'Validar y Fijar Escala' está activo, la venta proyectada es proporcional al peso real del SKU en la empresa.")
        
        # Mostrar tabla
        st.dataframe(
            tactical[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_2025', 'VTA_PROY_2026', 'VTA_MENSUAL', 'MOS']]
            .sort_values('VTA_PROY_2026', ascending=False), 
            use_container_width=True
        )

else:
    st.warning("Aguardando conexión con Google Drive o archivos CSV...")

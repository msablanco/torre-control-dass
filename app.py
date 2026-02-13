import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="FILA - Forecast Control", layout="wide")

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
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error carga Drive: {e}")
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

    # --- SIDEBAR: PARÁMETROS Y EL NUEVO BOTÓN DE VALIDACIÓN ---
    st.sidebar.title("🎮 CONTROL DE FORECAST")
    
    # 1. Definir Objetivo
    target_vol = st.sidebar.number_input("Volumen Total Objetivo 2026", value=700000, step=50000)
    
    # 2. CUADRO DE VALIDACIÓN (EL BOTÓN QUE SOLICITASTE)
    validar_forecast = st.sidebar.checkbox("🔒 VALIDAR Y CONGELAR PROYECCIÓN", value=False, 
                                           help="Al tildar, el prorrateo se fija sobre el total de la empresa y no cambia al filtrar SKUs.")

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 FILTROS DE VISTA")
    search_query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp = st.sidebar.multiselect("Filtrar por Emprendimiento", opciones_emp)

    # --- 3. LÓGICA DE CÁLCULO BLINDADO ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    venta_total_real_2025 = so_2025['CANTIDAD'].sum()

    # Si está tildado, el factor es (Objetivo / Total Empresa) -> INVARIABLE
    if validar_forecast:
        factor_final = target_vol / venta_total_real_2025 if venta_total_real_2025 > 0 else 1
        st.sidebar.success(f"Cálculo fijado sobre {venta_total_real_2025:,.0f} unidades.")
    else:
        # Si no está tildado, recalcula según lo que ve (comportamiento que no querías)
        venta_actual = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['CANTIDAD'].sum() if f_emp else venta_total_real_2025
        factor_final = target_vol / venta_actual if venta_actual > 0 else 1
        st.sidebar.warning("⚠️ Proyección dinámica activa")

    # --- 4. CONSTRUCCIÓN DE LA TABLA TACTICAL ---
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})

    # Unión maestra (tactical siempre se define aquí para evitar NameError)
    tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)

    # Aplicamos el factor (fijo o dinámico según el botón)
    tactical['VTA_PROY_2026'] = (tactical['VTA_2025'] * factor_final).round(0)
    tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # --- 5. FILTRADO DE VISTA (NO AFECTA AL CÁLCULO) ---
    df_vista = tactical.copy()
    if f_emp:
        # Obtenemos los SKUs que pertenecen a ese emprendimiento en 2025
        skus_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['SKU'].unique()
        df_vista = df_vista[df_vista['SKU'].isin(skus_canal)]
    
    if search_query:
        df_vista = df_vista[df_vista['SKU'].str.contains(search_query) | df_vista['DESCRIPCION'].str.contains(search_query)]

    # --- INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario")
        # Mostramos solo registros con actividad
        df_ver = df_vista[(df_vista['STOCK'] > 0) | (df_vista['VTA_2025'] > 0)]
        
        st.dataframe(df_ver[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_2025', 'VTA_MENSUAL', 'MOS']]
                     .sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Detalle de Escenario")
        # Aquí tactical existe siempre, así que no habrá NameError
        total_proy = df_ver['VTA_PROY_2026'].sum()
        st.info(f"La selección actual representa una proyección anual de {total_proy:,.0f} unidades.")

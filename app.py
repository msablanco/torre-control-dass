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
        st.error(f"Error carga: {e}")
        return {}

data = load_drive_data()

if data:
    # 1. Preparación de Dataframes base
    sell_out = data.get('Sell_Out', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- SIDEBAR: PARÁMETROS Y VALIDACIÓN ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 100000, 2000000, 700000, step=50000)
    
    # CUADRO DE VALIDACIÓN (TU PEDIDO)
    # Al tildar esto, se "ancla" el cálculo de volumen global
    validar_forecast = st.sidebar.checkbox("✅ VALIDAR Y CONGELAR PROYECCIÓN", value=False)

    # Filtros de abajo (Solo afectan la vista si el checkbox está activo)
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

    # --- 2. LÓGICA DE BLINDAJE ---
    # Calculamos la venta total 2025 para el prorrateo
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    venta_total_real_2025 = so_2025['CANTIDAD'].sum()

    # Si se tilda el cuadro, calculamos el factor sobre el total de la empresa
    # y lo guardamos en session_state para que no cambie al filtrar.
    if validar_forecast:
        factor_escalamiento = target_vol / venta_total_real_2025 if venta_total_real_2025 > 0 else 1
        st.session_state['factor_fijo'] = factor_escalamiento
        st.sidebar.success(f"PROYECCIÓN BLINDADA (Factor: {factor_escalamiento:.4f})")
    else:
        # Si no está validado, usa el volumen de lo que haya filtrado (comportamiento original)
        venta_filtrada = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['CANTIDAD'].sum() if f_emp else venta_total_real_2025
        st.session_state['factor_fijo'] = target_vol / venta_filtrada if venta_filtrada > 0 else 1
        st.sidebar.warning("⚠️ Proyección dinámica (se recalcula al filtrar)")

    # --- 3. PROCESAMIENTO DE TABLA (TACTICAL) ---
    # Consolidamos Stock y Venta por SKU
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})

    # Unión maestra
    tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)

    # APLICACIÓN DEL FACTOR BLINDADO
    f_final = st.session_state['factor_fijo']
    tactical['VTA_PROY_2026'] = (tactical['VTA_25'] * f_final).round(0)
    tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    
    # MOS Corregido contra infinitos
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # --- 4. FILTRADO DE VISTA ---
    # Los filtros de canal y búsqueda solo afectan qué filas se muestran, NO el cálculo anterior
    df_vista = tactical.copy()
    if f_emp:
        # Para filtrar por canal en la vista, necesitamos traer la info de emprendimiento al tactical
        skus_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['SKU'].unique()
        df_vista = df_vista[df_vista['SKU'].isin(skus_canal)]
    
    if search_query:
        df_vista = df_vista[df_vista['SKU'].str.contains(search_query) | df_vista['DESCRIPCION'].str.contains(search_query)]

    # --- INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (Blindada)")
        st.dataframe(df_vista[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_25', 'VTA_MENSUAL', 'MOS']]
                     .sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        # Aquí evitamos el NameError asegurando que tactical existe siempre
        st.subheader("🔮 Detalle de Proyección")
        if not df_vista.empty:
            st.write(f"Venta Proyectada del segmento seleccionado: {df_vista['VTA_PROY_2026'].sum():,.0f} unidades.")
            st.write(f"Representa el {(df_vista['VTA_PROY_2026'].sum() / target_vol):.1%} del objetivo total.")

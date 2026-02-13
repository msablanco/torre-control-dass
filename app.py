import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="FILA - Forecast Blindado", layout="wide")

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
            if 'SKU' in df.columns: 
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
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
    
    # Objetivo
    target_vol = st.sidebar.number_input("Volumen Total Objetivo 2026", value=700000, step=50000)
    
    # --- EL CUADRO DE VALIDACIÓN QUE PEDISTE ---
    # Este es el interruptor que habilita o deshabilita el recalculo
    validar_forecast = st.sidebar.checkbox("✅ VALIDAR PROYECCIÓN (CONGELAR)", value=False, 
                                           help="Tildar para que la proyección no cambie al usar los filtros de abajo.")

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 FILTROS DE VISTA")
    search_query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

    # --- 2. LÓGICA DE BLINDAJE ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    
    # Calculamos la base de prorrateo
    if validar_forecast:
        # SI ESTÁ TILDADO: La base es el total de la empresa (No cambia al filtrar)
        base_prorrateo = so_2025['CANTIDAD'].sum()
        st.sidebar.success(f"PROYECCIÓN FIJA: Calculada sobre {base_prorrateo:,.0f} unidades.")
    else:
        # SI NO ESTÁ TILDADO: La base es lo que esté filtrado (comportamiento actual)
        df_temp = so_2025.copy()
        if f_emp:
            df_temp = df_temp[df_temp['EMPRENDIMIENTO'].isin(f_emp)]
        if search_query:
            df_temp = df_temp[df_temp['SKU'].str.contains(search_query)]
        
        base_prorrateo = df_temp['CANTIDAD'].sum()
        st.sidebar.warning("⚠️ PROYECCIÓN DINÁMICA: Cambia según los filtros.")

    # FACTOR DE ESCALA (Blindado o Dinámico)
    factor_final = target_vol / base_prorrateo if base_prorrateo > 0 else 1

    # --- 3. PROCESAMIENTO DE TABLA TACTICAL ---
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})

    # Unión maestra (Fuera de tabs para evitar NameError)
    tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)

    # Cálculo de proyección usando el factor (Fijo o Variable según el tilde)
    tactical['VTA_PROY_2026'] = (tactical['VTA_2025'] * factor_final).round(0)
    tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # --- 4. FILTRADO DE VISTA (SOLO ESTÉTICO) ---
    df_vista = tactical.copy()
    if f_emp:
        skus_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['SKU'].unique()
        df_vista = df_vista[df_vista['SKU'].isin(skus_canal)]
    if search_query:
        df_vista = df_vista[df_vista['SKU'].str.contains(search_query) | df_vista['DESCRIPCION'].str.contains(search_query)]

    # --- 5. INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        st.dataframe(df_vista[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_2025', 'VTA_MENSUAL', 'MOS']]
                     .sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        # Aquí tactical existe siempre, solucionando el NameError de tus capturas
        st.subheader("🔮 Validación de Volumen")
        total_proyectado_vista = df_vista['VTA_PROY_2026'].sum()
        st.metric("Total Proyectado en Vista", f"{total_proyectado_vista:,.0f}")
        st.write(f"Este segmento representa el **{(total_proyectado_vista/target_vol):.1%}** del objetivo de 2026.")

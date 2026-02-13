import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="FILA - Forecast Blindado", layout="wide")

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
    # 1. Preparación de datos base
    sell_out = data.get('Sell_Out', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- SIDEBAR: ESTO DEBE APARECER PRIMERO ---
    st.sidebar.header("🎯 CONTROL DE VOLUMEN")
    
    vol_obj = st.sidebar.number_input("Volumen Total Objetivo 2026", value=1000000, step=50000)
    
    # EL BOTÓN DE VALIDACIÓN (CHECKBOX)
    # Si lo marcas, los números se quedan fijos aunque filtres.
    validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 FILTROS DE VISTA")
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if not sell_out.empty else []
    f_emp = st.sidebar.multiselect("Canal / Emprendimiento", opciones_emp)

    # --- 2. LÓGICA DE ESCALAMIENTO BLINDADA ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy() if not sell_out.empty else pd.DataFrame()
    
    # Venta total real de toda la empresa en 2025
    venta_total_empresa = so_2025['CANTIDAD'].sum() if not so_2025.empty else 0

    if validar_fijar:
        # SI ESTÁ MARCADO: El factor se calcula sobre el TOTAL DE LA EMPRESA
        # No importa qué busques en el cuadro de texto, este número no cambia.
        factor_escala = vol_obj / venta_total_empresa if venta_total_empresa > 0 else 1
        st.sidebar.success(f"🔒 ESCALA FIJADA: {factor_escala:.4f}")
    else:
        # SI ESTÁ DESMARCADO: Recalcula según lo filtrado (el error que tenías)
        df_temp = so_2025.copy()
        if f_emp:
            df_temp = df_temp[df_temp['EMPRENDIMIENTO'].isin(f_emp)]
        if query:
            df_temp = df_temp[df_temp['SKU'].str.contains(query)]
        
        venta_en_pantalla = df_temp['CANTIDAD'].sum() if not df_temp.empty else 0
        factor_escala = vol_obj / venta_en_pantalla if venta_en_pantalla > 0 else 1
        st.sidebar.warning("⚠️ ESCALA DINÁMICA (Recalculando)")

    # --- 3. PROCESAMIENTO TACTICAL (MOS) ---
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})

    # Generamos la tabla 'tactical' siempre aquí para evitar el NameError
    tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)

    # Aplicamos la proyección
    tactical['VTA_PROY_ANUAL'] = (tactical['VTA_2025'] * factor_escala).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)

    # --- 4. FILTRADO DE VISTA (SOLO OCULTA FILAS, NO AFECTA EL CÁLCULO) ---
    df_vista = tactical.copy()
    if f_emp:
        skus_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)]['SKU'].unique()
        df_vista = df_vista[df_vista['SKU'].isin(skus_canal)]
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query)]

    # --- INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 DETALLE"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        st.dataframe(df_vista[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_2025', 'VTA_PROY_MENSUAL', 'MOS']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Validación de Proyección")
        total_proy = df_vista['VTA_PROY_ANUAL'].sum()
        st.write(f"Venta Proyectada Anual para esta vista: **{total_proyectado:,.0f}**")
        st.write(f"Peso sobre el objetivo total: **{(total_proy/vol_obj):.1%}**")

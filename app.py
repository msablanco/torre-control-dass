import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

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
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates(subset=['SKU'])
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())

    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    # --- SIDEBAR: BLINDAJE DE PROYECCIÓN ---
    st.sidebar.title("🔒 BLINDAR PROYECCIÓN")
    target_vol = st.sidebar.number_input("Volumen Total Objetivo 2026", value=700000, step=50000)
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique())
    f_emp_base = st.sidebar.multiselect("Seleccionar Canal Base (Denominador)", opciones_emp)

    # BOTÓN CRÍTICO: Este botón congela el factor de escala
    if st.sidebar.button("🔒 FIJAR CÁLCULO Y BLINDAR"):
        so_25 = sell_out[sell_out['AÑO'] == 2025].copy()
        if f_emp_base:
            so_25 = so_25[so_25['EMPRENDIMIENTO'].isin(f_emp_base)]
        
        venta_total_base = so_25['CANTIDAD'].sum()
        # El factor se guarda en la sesión del usuario
        st.session_state['factor_blindado'] = target_vol / venta_total_base if venta_total_base > 0 else 1
        st.session_state['canales_blindados'] = f_emp_base
        st.sidebar.success(f"Proyección fijada. Factor: {st.session_state['factor_blindado']:.4f}")

    # --- SIDEBAR: FILTROS DE BÚSQUEDA ---
    st.sidebar.markdown("---")
    st.sidebar.title("🔍 FILTROS DE VISTA")
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # --- PROCESAMIENTO DE DATOS ---
    factor = st.session_state.get('factor_blindado', 1.0)
    canales = st.session_state.get('canales_blindados', [])

    # Obtener venta 2025 por SKU (Solo del canal base blindado)
    so_25_sku = sell_out[sell_out['AÑO'] == 2025].copy()
    if canales:
        so_25_sku = so_25_sku[so_25_sku['EMPRENDIMIENTO'].isin(canales)]
    
    vta_agrupada = so_25_sku.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})
    stk_agrupado = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})

    # Construir tabla maestra
    tactical = maestro.merge(vta_agrupada, on='SKU', how='left') \
                      .merge(stk_agrupado, on='SKU', how='left').fillna(0)

    # CÁLCULOS BLINDADOS: No dependen de lo que se filtre en pantalla
    tactical['VTA_PROY_ANUAL'] = (tactical['VTA_2025'] * factor).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
    
    # MOS: Corregido para evitar -inf o 0.0 cuando no hay venta
    tactical['MOS'] = (tactical['STK_ACTUAL'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), -float('inf')], 0).fillna(0).round(1)

    # --- APLICAR FILTRO DE VISTA ---
    df_vista = tactical.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query)]

    # --- INTERFAZ ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        if 'factor_blindado' not in st.session_state:
            st.warning("⚠️ La proyección no está blindada. Presiona el botón 'FIJAR CÁLCULO' para estabilizar los números.")
        
        # Filtro final para no mostrar filas vacías
        df_display = df_vista[(df_vista['STK_ACTUAL'] > 0) | (df_vista['VTA_2025'] > 0)]
        
        # KPIs Superiores
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs Filtrados", len(df_display))
        c2.metric("Factor Aplicado", f"{factor:.2f}x")
        avg_mos = df_display[df_display['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("Stock Promedio (MOS)", f"{avg_mos:.1f} meses" if not pd.isna(avg_mos) else "0.0 meses")

        st.dataframe(df_display[['SKU', 'DESCRIPCION', 'STK_ACTUAL', 'VTA_2025', 'VTA_PROY_MENSUAL', 'MOS']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        # Se define tactical arriba para que no haya NameError
        st.subheader("🔮 Detalle Individual")
        sku_list = df_display[df_display['VTA_PROY_MENSUAL'] > 0]['SKU'].unique()
        if len(sku_list) > 0:
            sku_sel = st.selectbox("Seleccionar SKU", sku_list)
            # Aquí podrías poner el gráfico de barras individual
        else:
            st.info("Filtra un SKU con ventas para ver el detalle.")

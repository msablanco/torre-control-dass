import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="FILA - Forecast Control", layout="wide")

# --- CARGA DE DATOS (CONEXIÓN DRIVE) ---
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
        st.error(f"Error cargando datos: {e}")
        return {}

data = load_drive_data()

if data:
    # Asignación de DataFrames
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Procesamiento de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['AÑO'] = df['FECHA_DT'].dt.year
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')

    # --- SIDEBAR: PARÁMETROS CRÍTICOS ---
    st.sidebar.header("🎯 CONFIGURACIÓN OBJETIVO")
    target_vol = st.sidebar.number_input("Volumen Objetivo 2026", value=700000, step=50000)
    
    # Filtro de Canal (Define la BASE del cálculo)
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Seleccionar Canal (Base de Prorrateo)", opciones_emp)

    # --- 1. LÓGICA DE BLINDAJE (EL CORAZÓN DEL PROBLEMA) ---
    # Calculamos la venta total del canal ANTES de filtrar por SKU
    so_2025_base = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp:
        so_2025_base = so_2025_base[so_2025_base['EMPRENDIMIENTO'].isin(f_emp)]
    
    venta_total_canal = so_2025_base['CANTIDAD'].sum()
    
    # FACTOR DE CRECIMIENTO: Es constante para todos los SKUs del canal
    FACTOR_CRECIMIENTO = target_vol / venta_total_canal if venta_total_canal > 0 else 1

    # --- 2. FILTROS DE VISTA (PARA EL BUSCADOR) ---
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 FILTROS DE BÚSQUEDA")
    search_query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # Filtrar el maestro según la búsqueda
    m_filt = maestro.copy()
    if search_query:
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]

    # --- 3. CONSOLIDACIÓN DE DATOS POR SKU ---
    # Agrupamos ventas del 2025 (del canal seleccionado) para cada SKU
    vta_sku = so_2025_base.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_ACTUAL'})
    ing_sku = ingresos.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS_FUT'})

    # Unimos todo partiendo del maestro (Deduplicado para evitar filas repetidas)
    tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_sku, on='SKU', how='left') \
                         .merge(vta_sku, on='SKU', how='left') \
                         .merge(ing_sku, on='SKU', how='left').fillna(0)

    # --- 4. CÁLCULOS BLINDADOS (NO CAMBIAN AL FILTRAR) ---
    # La proyección de un SKU es su propia venta * el factor del canal completo
    tactical['VTA_PROY_ANUAL'] = (tactical['VTA_2025'] * FACTOR_CRECIMIENTO).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
    
    # Cálculo de MOS (Meses de Stock)
    tactical['MOS'] = (tactical['STOCK_ACTUAL'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), -float('inf')], 0).fillna(0).round(1)

    # Clasificación de Estado
    def definir_estado(row):
        if row['VTA_PROY_MENSUAL'] == 0: return "⚪ SIN DEMANDA"
        if row['MOS'] < 2.5: return "🔥 QUIEBRE"
        if row['MOS'] > 8: return "⚠️ SOBRE-STOCK"
        return "✅ SALUDABLE"

    tactical['ESTADO'] = tactical.apply(definir_estado, axis=1)

    # --- INTERFAZ DE USUARIO (TABS) ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 DETALLE"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (Blindada)")
        
        # Filtro de limpieza para no mostrar SKUs sin nada
        df_ver = tactical[(tactical['STOCK_ACTUAL'] > 0) | (tactical['VTA_2025'] > 0)]
        
        # Indicadores en la parte superior
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("SKUs en Pantalla", len(df_ver))
        c2.metric("Factor Crecimiento", f"{FACTOR_CRECIMIENTO:.2f}x")
        c3.metric("Venta Proy. Mensual (Filtro)", f"{df_ver['VTA_PROY_MENSUAL'].sum():,.0f}")
        
        mos_mediano = df_ver[df_ver['VTA_PROY_MENSUAL'] > 0]['MOS'].median()
        c4.metric("MOS Mediano", f"{mos_mediano:.1f} meses")

        # Tabla Principal
        st.dataframe(
            df_ver[['SKU', 'DESCRIPCION', 'STOCK_ACTUAL', 'VTA_2025', 'INGRESOS_FUT', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
            .sort_values('VTA_PROY_MENSUAL', ascending=False),
            use_container_width=True
        )

    with tab3:
        st.subheader("🔮 Análisis Individual por SKU")
        skus_con_venta = df_ver[df_ver['VTA_2025'] > 0]['SKU'].unique()
        if len(skus_con_venta) > 0:
            sku_sel = st.selectbox("Seleccionar SKU para detalle:", skus_con_venta)
            detalle = df_ver[df_ver['SKU'] == sku_sel].iloc[0]
            st.write(f"**Descripción:** {detalle['DESCRIPCION']}")
            st.write(f"**Venta Real 2025:** {detalle['VTA_2025']:,.0f} unidades")
            st.write(f"**Proyección 2026 Mensual:** {detalle['VTA_PROY_MENSUAL']:,.0f} unidades")
        else:
            st.warning("No hay productos con venta histórica en los filtros actuales.")

else:
    st.error("No se pudieron cargar los archivos desde Google Drive. Revisa st.secrets.")

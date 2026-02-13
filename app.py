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
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 100000, 2000000, 700000, step=50000)
    
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])

    # --- 1. CÁLCULO DEL FACTOR DE ESCALA (ANCLADO AL CANAL) ---
    # Esto evita que la proyección se dispare al filtrar SKUs.
    so_2025_full_canal = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp:
        so_2025_full_canal = so_2025_full_canal[so_2025_full_canal['EMPRENDIMIENTO'].isin(f_emp)]
    
    total_venta_canal_2025 = so_2025_full_canal['CANTIDAD'].sum()
    # El factor se basa en el TOTAL del canal, no en lo que ves en pantalla
    FACTOR_REAL = target_vol / total_venta_canal_2025 if total_venta_canal_2025 > 0 else 1

    # --- 2. FILTRADO PARA LA VISTA ---
    m_filt = maestro.copy()
    if search_query: 
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    
    # Agrupamos datos para eliminar duplicados de SKUs
    so_vta = sell_out[(sell_out['AÑO'] == 2025) & (sell_out['SKU'].isin(m_filt['SKU']))]
    if f_emp: so_vta = so_vta[so_vta['EMPRENDIMIENTO'].isin(f_emp)]
    
    vta_agrupada = so_vta.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_2025'})
    stk_agrupado = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})

    # Unión final para la tabla Tactical
    tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_agrupado, on='SKU', how='left') \
                     .merge(vta_agrupada, on='SKU', how='left').fillna(0)
    
    # Cálculos corregidos
    tactical['VTA_PROY_2026'] = (tactical['VTA_2025'] * FACTOR_REAL).round(0)
    tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_2026'] / 12).round(0)
    
    # MOS blindado contra ceros e infinitos
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)
    
    def clasificar_estado(r):
        if r['VTA_PROY_MENSUAL'] == 0: return "✅ SIN DEMANDA"
        return "🔥 QUIEBRE" if r['MOS'] < 2.5 else ("⚠️ SOBRE-STOCK" if r['MOS'] > 8 else "✅ SALUDABLE")
    
    tactical['ESTADO'] = tactical.apply(clasificar_estado, axis=1)

    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # Filtro para no mostrar SKUs sin stock ni venta
        df_display = tactical[(tactical['STOCK'] > 0) | (tactical['VTA_2025'] > 0)].copy()
        
        # KPIs Superiores Corregidos
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs en Riesgo", len(df_display[df_display['ESTADO'] == "🔥 QUIEBRE"]))
        c2.metric("SKUs con Exceso", len(df_display[df_display['ESTADO'] == "⚠️ SOBRE-STOCK"]))
        
        # Promedio MOS sin errores de -inf
        promedio_mos = df_display[df_display['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("Stock Promedio (MOS)", f"{promedio_mos:.1f} meses" if not pd.isna(promedio_mos) else "0.0 meses")

        st.dataframe(df_display[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_2025', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Detalle por SKU")
        sku_list = tactical[tactical['VTA_2025'] > 0]['SKU'].unique()
        if len(sku_list) > 0:
            sku_sel = st.selectbox("Seleccionar SKU", sku_list)
            # Aquí el código ya no dará NameError porque 'tactical' se definió arriba
        else:
            st.info("No hay datos de proyección para los filtros actuales.")

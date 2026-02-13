import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control Forecast", layout="wide")

# --- 1. CARGA DE DATOS ---
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
                if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
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

    # Procesamiento de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

 # --- 2. SIDEBAR: PARÁMETROS ---
    st.sidebar.title("🎮 PARÁMETROS")
    
    # Creamos dos conjuntos vacíos
    set_in = set(sell_in['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_in.columns else set()
    set_out = set(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else set()
    
    # Los unimos correctamente usando el símbolo | entre dos conjuntos (sets)
    opciones_emp = sorted(list(set_in | set_out))
    
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

    # --- 3. LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

    # --- 4. MOTOR DE CÁLCULO (UNIFICADO) ---
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}
    
    vta_tot_25 = so_filt[so_filt['AÑO'] == 2025]['CANTIDAD'].sum()
    factor_escala = target_vol / vta_tot_25 if vta_tot_25 > 0 else 1
    
    vta_sku_25 = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index()
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
    
    if not ingresos.empty:
        ing_futuros = ingresos.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'ING_FUTUROS'})
    else:
        ing_futuros = pd.DataFrame(columns=['SKU', 'ING_FUTUROS'])

    tactical = m_filt.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').merge(ing_futuros, on='SKU', how='left').fillna(0)
    tactical['VTA_PROY_MENSUAL'] = ((tactical['CANTIDAD'] * factor_escala) / 12).round(0)
    
    def calcular_mos_safe(row):
        if row['VTA_PROY_MENSUAL'] <= 0: return 0.0
        return round(row['STK_ACTUAL'] / row['VTA_PROY_MENSUAL'], 1)
    
    tactical['MOS'] = tactical.apply(calcular_mos_safe, axis=1)

    def clasificar_salud(row):
        if row['VTA_PROY_MENSUAL'] == 0: return "⚪ SIN VENTA"
        if row['MOS'] < 2.5: return "🔥 QUIEBRE"
        if row['MOS'] > 8: return "⚠️ SOBRE-STOCK"
        return "✅ SALUDABLE"
    
    tactical['ESTADO'] = tactical.apply(clasificar_salud, axis=1)

# --- BLOQUE FINAL DE RENDERIZADO (LÍNEA 200 EN ADELANTE) ---

tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

with tab1:
    st.subheader("Análisis de Demanda y Proyección")
    if 'fig_perf' in locals():
        st.plotly_chart(fig_perf, use_container_width=True, key="grafico_tab_1_perf")
    else:
        st.warning("Gráfico de performance no disponible.")

with tab2:
    st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
    if 'tactical' in locals() and not tactical.empty:
        st.dataframe(tactical.set_index('SKU'), use_container_width=True)

with tab3:
    st.subheader("🔮 Línea de Tiempo de Oportunidad")
    if 'tactical' in locals() and not tactical.empty:
        sku_list = tactical['SKU'].unique()
        sku_sel = st.selectbox("Seleccionar SKU", sku_list, key="selector_sku_tab_3")
        
        if sku_sel:
            # Aquí va el cálculo de fig_stk (asegúrate de que fig_stk se cree aquí)
            if 'fig_stk' in locals():
                st.plotly_chart(fig_stk, use_container_width=True, key="grafico_tab_3_stk")


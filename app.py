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
# 1. Obtenemos los valores de Sell In (si existe la columna)
if 'EMPRENDIMIENTO' in sell_in.columns:
    set_in = set(sell_in['EMPRENDIMIENTO'].dropna().unique())
else:
    set_in = set()

# 2. Obtenemos los valores de Sell Out (si existe la columna)
if 'EMPRENDIMIENTO' in sell_out.columns:
    set_out = set(sell_out['EMPRENDIMIENTO'].dropna().unique())
else:
    set_out = set()

# 3. Unimos y ordenamos
opciones_emp = sorted(list(set_in | set_out))

# 4. Definición de todos los controles del Sidebar (AQUÍ ESTÁ EL CAMBIO)
st.sidebar.title("🎮 PARÁMETROS")

# Definimos search_query ANTES de usarlo
search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()

# Definimos f_emp
f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

# Definimos f_cli (Clientes)
opciones_cli = sorted(sell_in['CLIENTE_NAME'].unique()) if 'CLIENTE_NAME' in sell_in.columns else []
f_cli = st.sidebar.multiselect("Clientes", opciones_cli)

# --- DEFINICIÓN SEGURA DE FRANJA DE PRECIO ---
if 'FRANJA_PRECIO' in maestro.columns:
    # 1. Obtenemos los valores únicos
    u_franja = maestro['FRANJA_PRECIO'].unique()
    # 2. Convertimos todo a texto y quitamos los valores nulos (NaN)
    opciones_franja = sorted([str(x) for x in u_franja if pd.notna(x)])
else:
    opciones_franja = []

f_franja = st.sidebar.multiselect("Franja de Precio", opciones_franja)

# Definimos target_vol
target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)


# --- 3. LÓGICA DE FILTRADO (AHORA SÍ, TODO TIENE NOMBRE) ---
m_filt = maestro.copy()

if search_query:
    m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]

if f_franja:
    m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

# Filtros para Sell In
si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
if f_emp:
    si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
if f_cli:
    si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

# Filtros para Sell Out
so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
if f_emp:
    so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
if f_cli:
    so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]
  # --- 4. MOTOR DE CÁLCULO DE GRÁFICOS (LO QUE RECUPERA LA VISIBILIDAD) ---

# 1. Agrupamos los datos filtrados por mes
si_graf = si_filt.groupby('MES_STR')['PARES'].sum().reset_index()
so_graf = so_filt.groupby('MES_STR')['PARES'].sum().reset_index()

# 2. Creamos el gráfico de Performance
import plotly.graph_objects as go

fig_perf = go.Figure()

# Línea de Sell In
fig_perf.add_trace(go.Scatter(
    x=si_graf['MES_STR'], y=si_graf['PARES'],
    name='Sell In (Ingreso)', mode='lines+markers',
    line=dict(color='#1f77b4', width=3)
))

# Línea de Sell Out
fig_perf.add_trace(go.Scatter(
    x=so_graf['MES_STR'], y=so_graf['PARES'],
    name='Sell Out (Salida)', mode='lines+markers',
    line=dict(color='#ff7f0e', width=3)
))

fig_perf.update_layout(
    title="Evolución Mensual de Ventas",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
)

# --- 5. TABLA TÁCTICA (Para la Tab 2) ---
# Aquí calculamos lo que necesita la pestaña TACTICAL
tactical = m_filt.copy()
# (Asegúrate de que aquí vayan tus cálculos de Stock, MOS, etc.)

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










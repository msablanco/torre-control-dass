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

    # --- SIDEBAR ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- 1. CÁLCULO DEL DENOMINADOR ESTÁTICO (CLAVE) ---
    # Este total NO se ve afectado por la búsqueda de SKU
    so_total_canal = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp: so_total_canal = so_total_canal[so_total_canal['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_total_canal = so_total_canal[so_total_canal['CLIENTE_NAME'].isin(f_cli)]
    
    venta_total_referencia = so_total_canal['CANTIDAD'].sum()
    
    # FACTOR FIJO: Se calcula una sola vez por ejecución
    FACTOR_ESTATICO = target_vol / venta_total_referencia if venta_total_referencia > 0 else 1

    # --- 2. FILTRADO PARA VISUALIZACIÓN (TABLAS Y GRÁFICOS) ---
    m_filt = maestro.copy()
    if search_query: 
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: 
        m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    def f_visual(df):
        if df.empty: return df
        temp = df[df['SKU'].isin(m_filt['SKU'])]
        if f_emp and 'EMPRENDIMIENTO' in temp.columns: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_cli and 'CLIENTE_NAME' in temp.columns: temp = temp[temp['CLIENTE_NAME'].isin(f_cli)]
        return temp

    si_v = f_visual(sell_in)
    so_v = f_visual(sell_out)
    st_v = f_visual(stock)
    in_v = f_visual(ingresos)

    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # SOLAPA 1: INTACTA
    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        si_25 = si_v[si_v['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_v[so_v['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        # PROYECCIÓN ESTÁTICA
        so_25['PROY_2026'] = (so_25['CANTIDAD'] * FACTOR_ESTATICO).round(0)
        
        df_plot = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]}).merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025"))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(width=4, color='#2ecc71')))
        st.plotly_chart(fig, use_container_width=True)

    # SOLAPA 2: NORMALIZADA Y FIJA
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        v_sku = so_v[so_v['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SO_25'})
        s_sku = st_v.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
        i_sku = in_v.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS_FUTUROS'})
        si_sku = si_v[si_v['AÑO'] == 2025].groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'SI_25'})

        tactical = m_filt.drop_duplicates(subset=['SKU']).merge(s_sku, on='SKU', how='left') \
                         .merge(v_sku, on='SKU', how='left') \
                         .merge(i_sku, on='SKU', how='left') \
                         .merge(si_sku, on='SKU', how='left').fillna(0)
        
        tactical = tactical[(tactical['STOCK'] > 0) | (tactical['SO_25'] > 0) | (tactical['INGRESOS_FUTUROS'] > 0)]
        
        # CÁLCULOS QUE NO CAMBIAN AL BUSCAR SKU
        tactical['VTA_PROY_ANUAL'] = (tactical['SO_25'] * FACTOR_ESTATICO).round(0)
        tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
        
        tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 99).fillna(0).round(1)
        
        tactical['ESTADO'] = tactical.apply(lambda r: "🔥 QUIEBRE" if r['MOS'] < 2.5 else ("⚠️ SOBRE-STOCK" if r['MOS'] > 8 else "✅ SALUDABLE"), axis=1)

        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STOCK', 'SO_25', 'SI_25', 'INGRESOS_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("🔮 Línea de Tiempo")
        sku_list = tactical['SKU'].unique()
        if len(sku_list) > 0:
            sku_sel = st.selectbox("Seleccionar SKU", sku_list)
            # Lógica de proyección de stock mensual aquí...

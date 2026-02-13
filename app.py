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
        st.error(f"Error en carga: {e}")
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
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- 1. PASO CLAVE: CALCULAR EL DENOMINADOR ESTÁTICO ---
    # Usamos TODO el Sell Out 2025 del canal, SIN filtrar por la búsqueda de SKU del usuario.
    so_referencia = sell_out[sell_out['AÑO'] == 2025].copy()
    if f_emp:
        so_referencia = so_referencia[so_referencia['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli:
        so_referencia = so_referencia[so_referencia['CLIENTE_NAME'].isin(f_cli)]
    
    venta_total_2025_canal = so_referencia['CANTIDAD'].sum()
    
    # FACTOR FIJO: Este factor se aplica a cada SKU individualmente
    FACTOR_PROPORCIONAL = target_vol / venta_total_2025_canal if venta_total_2025_canal > 0 else 1

    # --- 2. FILTRADO PARA VISUALIZACIÓN (TABLAS Y BUSCADOR) ---
    m_filt = maestro.copy()
    if search_query: 
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: 
        m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    def aplicar_filtros_vistas(df):
        if df.empty: return df
        temp = df[df['SKU'].isin(m_filt['SKU'])]
        if f_emp and 'EMPRENDIMIENTO' in temp.columns: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_cli and 'CLIENTE_NAME' in temp.columns: temp = temp[temp['CLIENTE_NAME'].isin(f_cli)]
        return temp

    si_vista = aplicar_filtros_vistas(sell_in)
    so_vista = aplicar_filtros_vistas(sell_out)
    st_vista = aplicar_filtros_vistas(stock)
    in_vista = aplicar_filtros_vistas(ingresos)

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # TAB 1: PERFORMANCE (Lógica intacta pero con factor fijo)
    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        si_25 = si_vista[si_vista['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_vista[so_vista['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        # Prorrateo basado en el factor fijo del canal
        so_25['PROY_2026'] = (so_25['CANTIDAD'] * FACTOR_PROPORCIONAL).round(0)
        
        df_plot = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]}).merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025"))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(width=4, color='#2ecc71')))
        st.plotly_chart(fig, use_container_width=True)

    # TAB 2: TACTICAL (NORMALIZADO Y BLINDADO)
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        v_agg = so_vista[so_vista['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        s_agg = st_vista.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
        i_agg = in_vista.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'ING_FUTUROS'})
        
        # Unimos partiendo del maestro deduplicado para evitar filas repetidas
        tactical = m_filt.drop_duplicates(subset=['SKU']).merge(s_agg, on='SKU', how='left') \
                         .merge(v_agg, on='SKU', how='left') \
                         .merge(i_agg, on='SKU', how='left').fillna(0)
        
        # Filtro de limpieza: No mostrar productos sin ningún movimiento ni stock
        tactical = tactical[(tactical['STOCK'] > 0) | (tactical['VTA_25'] > 0) | (tactical['ING_FUTUROS'] > 0)]
        
        # CÁLCULOS QUE NO SE RECALCULAN AL FILTRAR SKU
        tactical['VTA_PROY_ANUAL'] = (tactical['VTA_25'] * FACTOR_PROPORCIONAL).round(0)
        tactical['VTA_PROY_MENSUAL'] = (tactical['VTA_PROY_ANUAL'] / 12).round(0)
        
        # MOS: Evitamos divisiones por cero o negativos (reemplaza el -inf)
        tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), float('-inf')], 99).fillna(0).round(1)
        
        def clasificar(r):
            if r['VTA_PROY_MENSUAL'] == 0 and r['STOCK'] > 0: return "🔴 EXCESO"
            return "🔥 QUIEBRE" if r['MOS'] < 2.5 else ("⚠️ SOBRE-STOCK" if r['MOS'] > 8 else "✅ SALUDABLE")
        
        tactical['ESTADO'] = tactical.apply(clasificar, axis=1)

        # KPIs superiores corregidos
        k1, k2, k3 = st.columns(3)
        k1.metric("SKUs en Riesgo de Quiebre", len(tactical[tactical['ESTADO'] == "🔥 QUIEBRE"]))
        k2.metric("SKUs con Exceso", len(tactical[tactical['ESTADO'] == "🔴 EXCESO"]))
        # Promedio MOS solo de productos con venta proyectada para evitar el -inf
        mos_prom = tactical[tactical['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        k3.metric("Stock Promedio (MOS)", f"{mos_prom:.1f} meses" if not pd.isna(mos_prom) else "0.0 meses")

        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STOCK', 'VTA_25', 'ING_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

    # TAB 3: ESCENARIOS (Arregla el NameError)
    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_list = tactical.sort_values('VTA_PROY_MENSUAL', ascending=False)['SKU'].unique()
        if len(sku_list) > 0:
            sku_sel = st.selectbox("Seleccionar SKU para análisis detallado", sku_list)
            res = tactical[tactical['SKU'] == sku_sel].iloc[0]
            st.info(f"Análisis para: {res['DESCRIPCION']} | Proyección Mensual: {res['VTA_PROY_MENSUAL']:.0f} u.")

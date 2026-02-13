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
                if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
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

    # Procesamiento de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR: FILTROS UNIFICADOS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO DINÁMICO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    # Función para aplicar filtros de sidebar a cualquier dataframe
    def aplicar_filtros_globales(df):
        if df.empty: return df
        temp = df[df['SKU'].isin(m_filt['SKU'])]
        if f_emp and 'EMPRENDIMIENTO' in temp.columns: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_cli and 'CLIENTE_NAME' in temp.columns: temp = temp[temp['CLIENTE_NAME'].isin(f_cli)]
        return temp

    si_filt = aplicar_filtros_globales(sell_in)
    so_filt = aplicar_filtros_globales(sell_out)
    stk_filt = aplicar_filtros_globales(stock)
    ing_filt = aplicar_filtros_globales(ingresos)

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # SOLAPA 1: PERFORMANCE (Mantenida intacta)
    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        total_so_25 = so_25['CANTIDAD'].sum()
        if total_so_25 > 0:
            so_25['PROY_2026'] = ((so_25['CANTIDAD'] / total_so_25) * target_vol).round(0)
        else:
            so_25['PROY_2026'] = 0
        df_plot = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]}).merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025"))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(width=4)))
        st.plotly_chart(fig, use_container_width=True)

    # SOLAPA 2: TACTICAL (CORREGIDA SEGÚN PEDIDO)
   with tab2:
    st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
    
    # 1. FILTRAR TODO POR EMPRENDIMIENTO PRIMERO
    stk_f = stock[stock['SKU'].isin(m_filt['SKU'])]
    if f_emp and 'EMPRENDIMIENTO' in stock.columns:
        stk_f = stk_f[stk_f['EMPRENDIMIENTO'].isin(f_emp)]
        
    ing_f = ingresos[ingresos['SKU'].isin(m_filt['SKU'])]
    if f_emp and 'EMPRENDIMIENTO' in ingresos.columns:
        ing_f = ing_f[ing_f['EMPRENDIMIENTO'].isin(f_emp)]

    # 2. AGRUPAR PARA TENER 1 SOLA FILA POR SKU (EVITA REPETICIÓN)
    stk_sku = stk_f.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK'})
    vta_sku = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_OUT'})
    ing_sku = ing_f.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS_FUTUROS'})
    si_sku = si_filt[si_filt['AÑO'] == 2025].groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'SELL_IN'})

    # 3. UNIR TODO PARTIENDO DE UN MAESTRO SIN DUPLICADOS
    maestro_unico = m_filt.drop_duplicates(subset=['SKU'])
    
    tactical = maestro_unico.merge(stk_sku, on='SKU', how='left') \
                            .merge(vta_sku, on='SKU', how='left') \
                            .merge(ing_sku, on='SKU', how='left') \
                            .merge(si_sku, on='SKU', how='left').fillna(0)

    # 4. FILTRAR FILAS QUE NO TENGAN NINGÚN DATO (TU PEDIDO)
    tactical = tactical[
        (tactical['STOCK'] > 0) | 
        (tactical['SELL_OUT'] > 0) | 
        (tactical['SELL_IN'] > 0) | 
        (tactical['INGRESOS_FUTUROS'] > 0)
    ]

    # 5. CÁLCULO DE MOS Y ESTADO
    vta_tot_so = vta_sku['SELL_OUT'].sum()
    factor = target_vol / vta_tot_so if vta_tot_so > 0 else 1
    tactical['VTA_PROY_MENSUAL'] = ((tactical['SELL_OUT'] * factor) / 12).round(0)
    
    # Evitar el error de -inf meses
    tactical['MOS'] = (tactical['STOCK'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf')], 99).round(1)

    # KPIs (Solo los que pediste)
    c1, c2 = st.columns(2)
    c1.metric("SKUs en Riesgo de Quiebre", len(tactical[tactical['MOS'] < 2.5]))
    c2.metric("SKUs con Exceso", len(tactical[tactical['MOS'] > 8]))

    # MOSTRAR TABLA FINAL
    st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STOCK', 'SELL_OUT', 'INGRESOS_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']], use_container_width=True)

    # SOLAPA 3: ESCENARIOS (Activa)
    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_sel = st.selectbox("Seleccionar SKU", tactical.sort_values('STOCK', ascending=False)['SKU'].unique())
        if sku_sel:
            m_sku = tactical[tactical['SKU'] == sku_sel].iloc[0]
            st.write(f"Análisis para: {m_sku['DESCRIPCION']}")
            # Lógica de proyección de stock mensual...


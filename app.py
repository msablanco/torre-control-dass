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

    def apply_filters(df):
        if df.empty: return df
        temp = df[df['SKU'].isin(m_filt['SKU'])]
        if f_emp and 'EMPRENDIMIENTO' in temp.columns: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_cli and 'CLIENTE_NAME' in temp.columns: temp = temp[temp['CLIENTE_NAME'].isin(f_cli)]
        return temp

    si_filt = apply_filters(sell_in)
    so_filt = apply_filters(sell_out)
    stk_filt = apply_filters(stock)
    ing_filt = apply_filters(ingresos)

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # SOLAPA 1: PERFORMANCE (MANTENIDA)
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
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True)

    # SOLAPA 2: TACTICAL (CORREGIDA)
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # Agrupaciones por SKU filtradas por Emprendimiento
        vta_tot_25 = so_filt[so_filt['AÑO'] == 2025]['CANTIDAD'].sum()
        factor_escala = target_vol / vta_tot_25 if vta_tot_25 > 0 else 1
        
        vta_sku_25 = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_OUT_2025'})
        stk_sku = stk_filt.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
        ing_sku = ing_filt.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS_FUTUROS'})
        si_sku = si_filt[si_filt['AÑO'] == 2025].groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'SELL_IN_2025'})
        
        # Unión y limpieza de duplicados
        tactical = m_filt.drop_duplicates(subset=['SKU']).merge(stk_sku, on='SKU', how='left') \
                         .merge(vta_sku_25, on='SKU', how='left') \
                         .merge(ing_sku, on='SKU', how='left') \
                         .merge(si_sku, on='SKU', how='left').fillna(0)
        
        # Filtro: Solo mostrar si tiene algún dato
        tactical = tactical[(tactical['STK_ACTUAL'] > 0) | (tactical['SELL_OUT_2025'] > 0) | 
                            (tactical['SELL_IN_2025'] > 0) | (tactical['INGRESOS_FUTUROS'] > 0)]
        
        tactical['VTA_PROY_MENSUAL'] = ((tactical['SELL_OUT_2025'] * factor_escala) / 12).round(0)
        tactical['MOS'] = (tactical['STK_ACTUAL'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), -float('inf')], 99).round(1)
        
        def clasificar(row):
            if row['VTA_PROY_MENSUAL'] == 0 and row['STK_ACTUAL'] > 0: return "🔴 EXCESO/CLAVO"
            if row['MOS'] < 2.5: return "🔥 QUIEBRE"
            if row['MOS'] > 8: return "⚠️ SOBRE-STOCK"
            return "✅ SALUDABLE"
        
        tactical['ESTADO'] = tactical.apply(clasificar, axis=1)
        
        c1, c2 = st.columns(2)
        c1.metric("SKUs en Riesgo de Quiebre (Únicos)", len(tactical[tactical['ESTADO'] == "🔥 QUIEBRE"]))
        c2.metric("SKUs con Exceso (Únicos)", len(tactical[tactical['ESTADO'] == "⚠️ SOBRE-STOCK"]))

        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'STK_ACTUAL', 'SELL_OUT_2025', 'INGRESOS_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']]
                     .sort_values('VTA_PROY_MENSUAL', ascending=False)
                     .style.format({'STK_ACTUAL': '{:,.0f}', 'SELL_OUT_2025': '{:,.0f}', 'INGRESOS_FUTUROS': '{:,.0f}', 'VTA_PROY_MENSUAL': '{:,.0f}'}), 
                     use_container_width=True)

    # SOLAPA 3: ESCENARIOS (MANTENIDA Y CONECTADA)
    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_list = tactical.sort_values('VTA_PROY_MENSUAL', ascending=False)['SKU'].unique()
        sku_sel = st.selectbox("Seleccionar SKU", sku_list)
        if sku_sel:
            m_sku = tactical[tactical['SKU'] == sku_sel].iloc[0]
            ing_detalle = ing_filt[ing_filt['SKU'] == sku_sel].groupby('MES_STR')['UNIDADES'].sum()
            evol = []
            curr = m_sku['STK_ACTUAL']
            for i in range(1, 13):
                curr = (curr + ing_detalle.get(str(i).zfill(2), 0)) - m_sku['VTA_PROY_MENSUAL']
                evol.append(max(0, curr))
            fig_op = go.Figure()
            fig_op.add_trace(go.Scatter(x=list(meses_nombres.values()), y=evol, name="Stock", fill='tozeroy', line=dict(color='red')))
            st.plotly_chart(fig_op, use_container_width=True)

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
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_SI'})
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

    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- SIDEBAR ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Producto", "").upper()
    
    # NUEVO SLIDER: Volumen Total Objetivo
    target_vol = st.sidebar.slider("Volumen Total Objetivo Sell Out 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    f_emp = st.sidebar.multiselect("Emprendimiento", sell_in['EMPRENDIMIENTO'].unique() if 'EMPRENDIMIENTO' in sell_in.columns else [])
    f_cli_si = st.sidebar.multiselect("Sell In Clientes", sell_in['CLIENTE_SI'].unique() if 'CLIENTE_SI' in sell_in.columns else [])
    f_cli_so = st.sidebar.multiselect("Sell Out Clientes (Canal)", sell_out['CLIENTE'].unique() if 'CLIENTE' in sell_out.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli_si: si_filt = si_filt[si_filt['CLIENTE_SI'].isin(f_cli_si)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_cli_so: so_filt = so_filt[so_filt['CLIENTE'].isin(f_cli_so)]

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS SKU"])

    with tab1:
        st.subheader("Curva de Demanda y Forecast 2026")
        meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}
        
        # Agrupaciones
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        # Lógica de Distribución de Volumen Target
        total_so_25 = so_25['CANTIDAD'].sum()
        if total_so_25 > 0:
            so_25['PESO_MES'] = so_25['CANTIDAD'] / total_so_25
            so_25['PROY_2026'] = (so_25['PESO_MES'] * target_vol).round(0)
        else:
            so_25['PROY_2026'] = 0

        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True)

        # TABLA 1: DATOS CON TOTAL
        st.markdown("### 📋 Detalle de Valores Mensuales")
        df_resumen = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].copy()
        df_resumen.columns = ['Mes', 'Sell In 2025', 'Sell Out 2025', 'Proyección 2026']
        df_t1 = df_resumen.set_index('Mes').T
        df_t1['TOTAL'] = df_t1.sum(axis=1)
        st.dataframe(df_t1.style.format("{:,.0f}"), use_container_width=True)

        # TABLA 2: DISCIPLINA CON TOTAL
        st.markdown("### 🧪 Proyección 2026 por Disciplina")
        if not so_filt.empty and not m_filt.empty:
            so_disc = so_filt[so_filt['AÑO'] == 2025].merge(m_filt[['SKU', 'DISCIPLINA']], on='SKU')
            total_gen_25 = so_disc['CANTIDAD'].sum()
            
            disc_pivot = so_disc.groupby(['DISCIPLINA', 'MES_STR'])['CANTIDAD'].sum().reset_index()
            if total_gen_25 > 0:
                disc_pivot['PROY_2026'] = ((disc_pivot['CANTIDAD'] / total_gen_25) * target_vol).round(0)
            else:
                disc_pivot['PROY_2026'] = 0
                
            tabla_disc = disc_pivot.pivot(index='DISCIPLINA', columns='MES_STR', values='PROY_2026').fillna(0)
            tabla_disc.columns = [meses_nombres.get(col, col) for col in tabla_disc.columns]
            tabla_disc['TOTAL'] = tabla_disc.sum(axis=1)
            st.dataframe(tabla_disc.sort_values('TOTAL', ascending=False).style.format("{:,.0f}"), use_container_width=True)

    with tab2:
        st.subheader("Months of Stock (MOS)")
        vta_25_tot = so_filt[so_filt['AÑO'] == 2025]['CANTIDAD'].sum()
        vta_ref = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index()
        stk_act = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        
        ranking = m_filt.merge(stk_act, on='SKU', how='left').merge(vta_ref, on='SKU', how='left').fillna(0)
        # Factor de escala para MOS basado en el volumen total objetivo
        if vta_25_tot > 0:
            factor_escala = target_vol / vta_25_tot
        else:
            factor_escala = 1
            
        ranking['VTA_PROY_MENSUAL'] = ((ranking['CANTIDAD'] * factor_escala) / 12).round(0)
        ranking['MOS'] = (ranking['STK'] / ranking['VTA_PROY_MENSUAL']).replace([float('inf')], 99).round(1)
        st.dataframe(ranking[['SKU', 'DESCRIPCION', 'STK', 'VTA_PROY_MENSUAL', 'MOS']].sort_values('VTA_PROY_MENSUAL', ascending=False), use_container_width=True)

else:
    st.info("Esperando archivos...")

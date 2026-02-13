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

    # Normalización de Fechas
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
    growth_rate = st.sidebar.slider("% Var. Sell Out 2026 vs 2025", -100, 150, 0)
    
    st.sidebar.markdown("---")
    f_emp = st.sidebar.multiselect("Emprendimiento", sell_in['EMPRENDIMIENTO'].unique() if 'EMPRENDIMIENTO' in sell_in.columns else [])
    f_cli_si = st.sidebar.multiselect("Sell In Clientes", sell_in['CLIENTE_SI'].unique() if 'CLIENTE_SI' in sell_in.columns else [])
    f_cli_so = st.sidebar.multiselect("Sell Out Clientes (Canal)", sell_out['CLIENTE'].unique() if 'CLIENTE' in sell_out.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query:
        m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja:
        m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli_si: si_filt = si_filt[si_filt['CLIENTE_SI'].isin(f_cli_si)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_cli_so: so_filt = so_filt[so_filt['CLIENTE'].isin(f_cli_so)]

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS SKU"])

    with tab1:
        st.subheader("Curva de Demanda y Forecast 2026")
        
        # Mapeo de meses
        meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}
        
        # Agrupaciones para el gráfico
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        so_25['PROY_2026'] = (so_25['CANTIDAD'] * (1 + growth_rate/100)).round(0)
        
        # Asegurar que todos los meses estén presentes
        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección Sell Out 2026", line=dict(color='#2ecc71', width=4)))
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        # --- TABLA 1: DATOS MENSUALES ---
        st.markdown("### 📋 Detalle de Valores Mensuales")
        df_resumen = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].copy()
        df_resumen.columns = ['Mes', 'Sell In 2025', 'Sell Out 2025', 'Proyección 2026']
        # Estilo para miles
        st.dataframe(df_resumen.set_index('Mes').T.style.format("{:,.0f}"), use_container_width=True)

        # --- TABLA 2: PROYECCIÓN 2026 POR DISCIPLINA ---
        st.markdown("### 🧪 Proyección 2026 Aperturada por Disciplina")
        if not so_filt.empty and not m_filt.empty:
            # Cruzamos Sell Out con Maestro para tener Disciplina
            so_disc = so_filt[so_filt['AÑO'] == 2025].merge(m_filt[['SKU', 'DISCIPLINA']], on='SKU')
            
            # Agrupamos por Disciplina y Mes
            disc_pivot = so_disc.groupby(['DISCIPLINA', 'MES_STR'])['CANTIDAD'].sum().reset_index()
            
            # Aplicamos el factor de crecimiento a cada celda
            disc_pivot['PROY_2026'] = (disc_pivot['CANTIDAD'] * (1 + growth_rate/100)).round(0)
            
            # Creamos la tabla Pivot
            tabla_disciplina = disc_pivot.pivot(index='DISCIPLINA', columns='MES_STR', values='PROY_2026').fillna(0)
            
            # Renombramos columnas de '01' a 'Ene', etc.
            tabla_disciplina.columns = [meses_nombres.get(col, col) for col in tabla_disciplina.columns]
            
            st.dataframe(tabla_disciplina.style.format("{:,.0f}"), use_container_width=True)
        else:
            st.warning("No hay datos suficientes para abrir por Disciplina.")

    with tab2:
        st.subheader("Velocidad de Stock (MOS)")
        # ... (Resto del código de ranking se mantiene igual)
        vta_ref = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        stk_act = stock[stock['SKU'].isin(m_filt['SKU'])].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        ranking = m_filt.merge(stk_act, on='SKU', how='left').merge(vta_ref, on='SKU', how='left').fillna(0)
        ranking['VTA_PROY_26'] = (ranking['VTA_25'] * (1 + growth_rate/100)).round(0)
        ranking['MOS'] = (ranking['STK'] / ranking['VTA_PROY_26']).replace([float('inf')], 99).round(1)
        st.dataframe(ranking.sort_values('VTA_PROY_26', ascending=False), use_container_width=True)

else:
    st.info("Esperando archivos...")

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

    # --- SIDEBAR: FILTROS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO (ESTA PARTE ES CRÍTICA PARA SOLAPA 2) ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    def f_global(df):
        if df.empty: return df
        d = df[df['SKU'].isin(m_filt['SKU'])]
        if f_emp and 'EMPRENDIMIENTO' in d.columns: d = d[d['EMPRENDIMIENTO'].isin(f_emp)]
        if f_cli and 'CLIENTE_NAME' in d.columns: d = d[d['CLIENTE_NAME'].isin(f_cli)]
        return d

    si_f = f_global(sell_in)
    so_f = f_global(sell_out)
    st_f = f_global(stock) # Ahora el stock SÍ se filtra
    in_f = f_global(ingresos) # Ahora los ingresos SÍ se filtran

    # --- TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])
    meses_nom = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # SOLAPA 1: INTACTA
    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        si_25 = si_f[si_f['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_f[so_f['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        t_so = so_25['CANTIDAD'].sum()
        so_25['PROY_2026'] = ((so_25['CANTIDAD'] / t_so) * target_vol).round(0) if t_so > 0 else 0
        df_p = pd.DataFrame({'MES_STR':[str(i).zfill(2) for i in range(1,13)]}).merge(si_25,on='MES_STR',how='left').merge(so_25,on='MES_STR',how='left').fillna(0)
        df_p['MES_NOM'] = df_p['MES_STR'].map(meses_nom)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_p['MES_NOM'], y=df_p['UNIDADES'], name="Sell In 2025"))
        fig.add_trace(go.Scatter(x=df_p['MES_NOM'], y=df_p['CANTIDAD'], name="Sell Out 2025", line=dict(dash='dot')))
        fig.add_trace(go.Scatter(x=df_p['MES_NOM'], y=df_p['PROY_2026'], name="Proyección 2026", line=dict(width=4)))
        st.plotly_chart(fig, use_container_width=True)

    # SOLAPA 2: TACTICAL (REESCRITA)
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # 1. Agrupar datos por SKU antes del merge para evitar duplicados
        stk_agg = st_f.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD':'STOCK'})
        so_agg = so_f[so_f['AÑO']==2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD':'SELL_OUT'})
        in_agg = in_f.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES':'INGRESOS_FUTUROS'})
        
        # 2. Construir Matriz
        matriz = m_filt.drop_duplicates('SKU')[['SKU','DESCRIPCION','DISCIPLINA']].merge(stk_agg, on='SKU', how='left') \
                      .merge(so_agg, on='SKU', how='left') \
                      .merge(in_agg, on='SKU', how='left').fillna(0)
        
        # 3. Filtrar: Solo filas que tengan algún dato > 0
        matriz = matriz[(matriz['STOCK'] > 0) | (matriz['SELL_OUT'] > 0) | (matriz['INGRESOS_FUTUROS'] > 0)]
        
        # 4. Cálculo de Proyección Mensual y MOS
        t_so_global = so_agg['SELL_OUT'].sum()
        factor = target_vol / t_so_global if t_so_global > 0 else 1
        matriz['VTA_PROY_MES'] = ((matriz['SELL_OUT'] * factor) / 12).round(0)
        
        # Fix MOS y visualización
        matriz['MOS'] = (matriz['STOCK'] / matriz['VTA_PROY_MES']).replace([float('inf')], 99).round(1)
        
        # KPIs de SKUs únicos
        q1, q2 = st.columns(2)
        q1.metric("SKUs en Riesgo de Quiebre", len(matriz[matriz['MOS'] < 2.5]))
        q2.metric("SKUs con Exceso", len(matriz[matriz['MOS'] > 8]))

        st.dataframe(matriz.sort_values('VTA_PROY_MES', ascending=False), use_container_width=True)

    # SOLAPA 3: ESCENARIOS
    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_sel = st.selectbox("Seleccionar SKU", matriz['SKU'].unique())
        if sku_sel:
            m_sku = matriz[matriz['SKU']==sku_sel].iloc[0]
            stk_evol = []
            curr = m_sku['STOCK']
            for i in range(12):
                curr = max(0, curr - m_sku['VTA_PROY_MES'])
                stk_evol.append(curr)
            st.plotly_chart(go.Figure(go.Scatter(x=list(meses_nom.values()), y=stk_evol, fill='tozeroy')), use_container_width=True)
